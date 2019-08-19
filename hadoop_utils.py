#!/usr/bin/python

import subprocess
import logging
import os
import tempfile
import shutil
import json
import time

import hadoop_filestatus_utils

HDFS_CMD_PREFIX = "hadoop fs -"

__author__ = "Olivier Koch"
__copyright__ = ""
__credits__ = ["Olivier Koch"]
__license__ = "MIT"
__version__ = "1.0.1"
__maintainer__ = "Olivier Koch"
__email__ = "o.koch@criteo.com"
__status__ = "Prototype"


def hdfsRm(path):
    if hdfsFileExists(path):
        stdout, stderr, returnCode = runHdfsCmd("rm -r -skipTrash " + path)


def _chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def hdfsRmList(paths):
    logging.info("Start hdfsRmList on %s elements" % len(paths))
    if len(paths) > 0:
        chunkSize = 100
        for pathSubList in _chunks(paths, chunkSize):
            stdout, stderr, returnCode = runHdfsCmd(
                "rm -r -skipTrash " + " ".join(pathSubList))
    logging.info("End hdfsRmList")


def hdfsCat(path, retry_on_empty=False):
    stdout, stderr, returnCode = runHdfsCmd("text " + path)
    returnValue = stdout.split('\n')

    if retry_on_empty and (returnValue is None or len(returnValue) == 0 or len("".join(returnValue).strip()) == 0):
        logging.info(
            "hdfsCat result is suspiciously empty - Retrying in 15sec")
        time.sleep(15)
        stdout, stderr, returnCode = runHdfsCmd("text " + path)
        returnValue = stdout.split('\n')

    return returnValue


def hdfsCpFromLocal(localPath, hdfsPath):
    stdout, stderr, returnCode = runHdfsCmd(
        "copyFromLocal " + localPath + " " + hdfsPath)
    return returnCode


def hdfsCpToLocal(localPath, hdfsPath):
    if os.path.isfile(localPath):
        os.remove(localPath)
    stdout, stderr, returnCode = runHdfsCmd(
        "copyToLocal " + hdfsPath + " " + localPath)
    return returnCode


def hdfsTryCpToLocal(localPath, hdfsPath):
    try:
        if not hdfsFileExists(hdfsPath):
            return False
        return hdfsCpToLocal(localPath, hdfsPath) == 0
    except:
        return False


def hdfsMkdir(hdfsPath):
    stdout, stderr, returnCode = runHdfsCmd("mkdir -p " + hdfsPath)


def hdfsLs(hdfsPath):
    stdout, stderr, returnCode = runHdfsCmd("ls " + hdfsPath)
    cmdOutput = stdout.split('\n')
    result = parseHdfsLsOutput(cmdOutput)
    return result


def parseHdfsLsOutput(cmdOutput):
    result = []
    for line in cmdOutput:
        line = line.strip()
        splittedLine = filter(None, line.split(' '))
        if len(splittedLine) != 8:
            continue
        result.append(hadoop_filestatus_utils.FileStatus(*splittedLine))
    return result


def hdfsLsOnlyPaths(hdfsPath):
    return [filestatus.filename for filestatus in hdfsLs(hdfsPath)]


def hdfsFileExists(hdfsPath):
    stdout, stderr, returnCode = runHdfsCmd("test -e " + hdfsPath)
    return returnCode == 0


def writetoHdfs(finalFilePath, stringToWrite, tempDirectory=None, writeString=True):
    toLog = "Start writetoHdfs " + finalFilePath
    if writeString:
        toLog = toLog + " " + stringToWrite
    logging.info(toLog)
    if tempDirectory is not None and not os.path.exists(tempDirectory):
        os.makedirs(tempDirectory)
    with tempfile.NamedTemporaryFile(delete=False, dir=tempDirectory) as file:
        file.write(stringToWrite)
    hdfsRm(finalFilePath)

    returnCode = hdfsCpFromLocal(file.name, finalFilePath)
    nbTrials = 2
    while returnCode != 0 and nbTrials > 0:
        time.sleep(15)
        logging.info("Retrying copy, trial " + str(nbTrials))
        returnCode = hdfsCpFromLocal(file.name, finalFilePath)
        nbTrials -= 1

    shutil.rmtree(file.name, ignore_errors=True)
    logging.info("End writetoHdfs")


def writeDictToHdfs(finalFilePath, dict, tempDirectory=None):
    writetoHdfs(finalFilePath, json.dumps(
        dict, sort_keys=True, indent=2), tempDirectory, False)


def hdfsJsonfileToDict(pathToFile):
    try:
        return json.loads("\n".join(hdfsCat(pathToFile, True)))
    except:
        time.sleep(15)
        try:
            return json.loads("\n".join(hdfsCat(pathToFile, True)))
        except:
            logging.info(pathToFile + "Cannot be deserialized")
    logging.warning(pathToFile + " not found, returning {} instead")
    return {}


def runHdfsCmd(cmd):
    cmd = HDFS_CMD_PREFIX + cmd
    logging.info("Start command " + cmd)
    cmd = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    cmd_out, cmd_err = cmd.communicate()
    return_code = cmd.returncode

    std_err = cmd_err.strip()
    if len(std_err) > 0:
        for errLine in std_err.split('\n'):
            logging.warning(errLine)
    logging.info("End command with return code " + str(return_code))
    return (cmd_out.strip(), std_err, return_code)
