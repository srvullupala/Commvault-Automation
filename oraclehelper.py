"""
Date: June 6th 2017
Commited by: Shankar Vullupala
"""

import os, sys, cx_Oracle, time
from AutomationUtils import remoteconnection, cmdhelper, dbhelper
from AutomationUtils import loghelper
#from AutomationUtils.loghelper import log
from SimpanaUtils import clienthelper, instancehelper, apptypehelper
from SimpanaUtils import subclienthelper
from OperatingSystemUtils import Win_FS_Util, Registryhelper
from OperatingSystemUtils.UnixHelper import UnixHelper
import re
import AutomationConstants
import win32wnet, shutil, win32netcon, _winreg, win32con, winnt
from datetime import datetime

"""
Commented by Diptiman on 4th May 2015
def hasRACPseudoClientName(CommserverName,RACPseudoClientName):

    log = loghelper.getLog()
    try:

        cmd = "qlist client -cs " + CommserverName

        (_retcode, _retval) = cmdhelper.executeCommand(cmd)
        print _retcode
        returnTuple = cmdhelper.checkStringInListIgnoreCase(_retval, RACPseudoClientName)
        if returnTuple == (0, True):
            return True
        else:
            return False
    except:
        log.exception("Exception raised at hasRACPseudoClientName")
        return False
"""
"""
finds out if an Instance exists or not and returns the status
returns True on success, False on error or UE.
"""

"""Adding method to get csn name with given commserver name : By krithika"""

def get_csn(CommServerName):
    l1 = CommServerName.split(".")
    csn = l1[0]
    return csn

def hasInstance(CommserverName, RACPseudoClientName, DataAgentType, Instance):

    log = loghelper.getLog()
    try:

        cmd = "qlist instance -c " + RACPseudoClientName + " -a " + DataAgentType + " -cs " + CommserverName

        (_retcode, _retval) = cmdhelper.executeCommand(cmd)

        returnTuple = cmdhelper.checkStringInListIgnoreCase(_retval, Instance)
        if returnTuple == (0, True):
            return True
        else:
            return False
    except:
        log.exception("Exception raised at hasInstance")
        return False


"""
Function to Delete and Add Oracle RAC Pseudo Client and Oracle RAC Nodes
Start Section - Added by Diptiman Basak on 28th Feb 2014 for Oracle RAC Automation.
deletePseudoClient function is added on 4th May 2015 by Diptiman Basak
"""

def deletePseudoClient(CommServerName, RACPseudoClientName):
    
    log = loghelper.getLog()
    try:
        cmd = "qdelete client -c " + RACPseudoClientName + " -y" + " -CS " + CommServerName
        
        (retcode, retval) = cmdhelper.executeCommand(cmd)
        print retcode
        if retcode != 0 :
          log.info("Failed to Delete Oracle RAC Pseudo Client")
          return False
        else:
          log.info("Oracle RAC Pseudo Client Deleted")
          return True
    except:
        log.exception("Exception raised at deletePseudoClient")
        return False

def createRACPseudoClient(CommServerName, RACPseudoClientName):
    """
        creates RAC Pseudo Client and returns a boolean value indicating the status of the creation
        returns True on success, False on error and UE.
    """
    log = loghelper.getLog()
    try:

        cmd = "qoperation execute -af CreateRACClient.xml -CS " + CommServerName + " -clientInfo/clientType 'RAC' -entity/clientName " + RACPseudoClientName

        (retcode, retval) = cmdhelper.executeCommand(cmd)
        print retcode
        if retcode !=0 :
            if str(retval).find('Duplicate entry') > 0:
                log.info("RAC Pseudoclient [" + RACPseudoClientName + "] already exists.")
                return True;
            else:
                log.info("RAC Pseudo client creation failed " + RACPseudoClientName)
                return False
        else:
            log.info("RAC Pseudo client created " + RACPseudoClientName)
            return True
    except:
        log.exception("Exception raised at createRACPseudoClient")
        return False


def createRACNode1(CommServerName, RACPseudoClientName, RACPhysicalClient, Instance, OracleHome, \
                          ConnectUser, ConnectPassword, ConnectString, DefaultStoragePolicy, \
                          LogStoragePolicy, CmdStoragePolicy, CatalogConnectEnable, CatalogUser, \
                          CatalogPassword, CatalogConnectString, OracleUser, ControlFileAutoBackup, \
                          BlockSize, DisableRMAN, NetworkAgents, SoftwareCompression, EnableDDB, \
                          GenerateSignature, InstanceOracleSID, TNSNamesFilePath):
    """
        creates RAC First Node and returns a boolean value indicating the status of the creation
        returns True on success, False on error and UE.
        Added by Diptiman Basak on 28th Feb 2014 for Oracle RAC Automation.
    """
    log = loghelper.getLog()
    try:

        cmd = "qoperation execute -af CreateRACInstance.xml -CS " + CommServerName + " -instance/appName 'Oracle RAC' -instance/clientName " + RACPseudoClientName + " -instance/instanceName " + Instance + " -useCatalogConnect " + CatalogConnectEnable + " -oracleRACInstance/connectString/userName " + CatalogUser + " -oracleRACInstance/connectString/password " + CatalogPassword + " -oracleRACInstance/connectString/domainName " + CatalogConnectString + " -ctrlFileAutoBackupType " + ControlFileAutoBackup + " -blockSize " + BlockSize + " -disableRMAN " + DisableRMAN + " -networkAgents " + NetworkAgents + " -softwareCompression " + SoftwareCompression + " -dataBackupStoragePolicy/storagePolicyName " + DefaultStoragePolicy + " -logBackupStoragePolicy/storagePolicyName " + LogStoragePolicy + " -enableDeduplication " + EnableDDB + " -generateSignature " + GenerateSignature + " -racDBOperationType " 'ADD' " -instancePhysicalClient/clientName " + RACPhysicalClient + " -instanceOracleSID " + InstanceOracleSID + " -userAccount/userName " + OracleUser + " -racDBInstance/oracleHome '" + OracleHome + "'' -racDBInstance/connectString/userName " + ConnectUser + " -racDBInstance/connectString/password " + ConnectPassword + " -racDBInstance/connectString/serviceName " + ConnectString + " -tnsAdminFolder " + TNSNamesFilePath

        (retcode, retval) = cmdhelper.executeCommand(cmd)
        print retcode
        if retcode !=0 :
            if str(retval).find('already exists') > 0:
                log.info('First RAC Node already added')
                return True
            else:
                log.info("First RAC Node Addition Failed in " + RACPseudoClientName)
                return False
        else:
            log.info("First RAC Node Addition Succeded in " + RACPseudoClientName)
            return True
    except:
        log.exception("Exception raised at createRACPseudoClient")
        return False

def createRACNode2(CommServerName, RACPseudoClientName, RACPhysicalClient2, Instance, OracleHome, \
                          ConnectUser, ConnectPassword, ConnectString2, OracleUser, InstanceOracleSID2):
    """
        creates RAC Second Node and returns a boolean value indicating the status of the creation
        returns True on success, False on error and UE.
        Added by Diptiman Basak on 28th Feb 2014 for Oracle RAC Automation.
    """
    log = loghelper.getLog()
    try:

        cmd = "qoperation execute -af UpdateRACInstance.xml -CS " + CommServerName + " -instance/appName 'Oracle RAC' -instance/clientName " + RACPseudoClientName + " -instance/instanceName " + Instance +  " -racDBOperationType " 'ADD' " -instancePhysicalClient/clientName " + RACPhysicalClient2 + " -instanceOracleSID " + InstanceOracleSID2 + " -userAccount/userName " + OracleUser + " -racDBInstance/oracleHome '" + OracleHome + "'' -racDBInstance/connectString/userName " + ConnectUser + " -racDBInstance/connectString/password " + ConnectPassword + " -racDBInstance/connectString/serviceName " + ConnectString2

        (retcode, retval) = cmdhelper.executeCommand(cmd)
        print retcode
        if retcode !=0 :
            log.info("Second RAC Node Addition Failed in " + RACPseudoClientName)
            return False
        else:
            log.info("Second RAC Node Addition Succeded in " + RACPseudoClientName)
            return True
    except:
        log.exception("Exception raised at createRACPseudoClient")
        return False

def createRACSubclient(CommServerName,RACPseudoClientName,Instance,SubClientName,DefaultStoragePolicy,LogStoragePolicy):
    """
        creates a subClient and returns a boolean value indication status of creation
        Added by Diptiman Basak on 28th Feb 2014 for Oracle RAC Automation.
    """
    log = loghelper.getLog()
    try:
        cmd = "qoperation execute -af create_subclient_template.xml -CS " + CommServerName + " -appName 'Oracle RAC' -clientName " + RACPseudoClientName + " -instanceName " + Instance + " -subClientName " + SubClientName + " -dataBackupStoragePolicy/storagePolicyName " + DefaultStoragePolicy + " -logBackupStoragePolicy/storagePolicyName " + LogStoragePolicy

        (retcode, retval) = cmdhelper.executeCommand(cmd)
        print retcode
        if retcode !=0 :
            if str(retval).find('already exists') > 0:
                log.info("SubClient [" + SubClientName + "] already exists.")
                return True
            else:
                log.info("SubClient " + SubClientName + " Creation Failed --> " + RACPseudoClientName)
                return False
        else:
            log.info("SubClient " + SubClientName + " Creation Succeded --> " + RACPseudoClientName)
            return True
    except:
        log.exception("Exception raised at createRACSubclient")
        return False


"""
End Function to add RAC Pseudo Client and Oracle RAC Nodes
End Section - Added by Diptiman Basak on 28th Feb 2014 for Oracle RAC Automation.
"""

def createOracleInstance(CommServerName, ClientName, Instance, DataAgentType, OracleHome, \
                         ConnectUser, ConnectPassword, ConnectString, DefaultStoragePolicy, \
                         LogStoragePolicy, CmdStoragePolicy, CatalogConnectEnable, CatalogUser, \
                         CatalogPassword, CatalogConnectString, OracleUser, OracleUserPassword):
        """
            creates an instance and returns a boolean value indicating the status of the creation
            returns True on success, False on error and UE.
        """

        log = loghelper.getLog()

        scriptspath = cmdhelper.getScriptsPath()
        xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\CreateInstance_Template.xml")

        try:
                    log.info("Trying create instance using XML")
                    if CatalogConnectEnable.lower() == 'yes':
                                cmd = "qoperation execute -af " + xmlfile + " -appName Oracle -CS " + CommServerName + "  -clientName " + ClientName + " -instanceName " + Instance + " -dataArchiveGroup/storagePolicyName " + DefaultStoragePolicy + " -commandLineStoragePolicy/storagePolicyName "+ CmdStoragePolicy  + " -logBackupStoragePolicy/StoragePolicyName " + LogStoragePolicy + " -oracleHome " + "\'"  + OracleHome + "\'" + " -oracleUser/userName " + OracleUser + " -sqlConnect/userName " + ConnectUser + " -sqlConnect/domainName " + ConnectString +  " -sqlconnect/password " + ConnectPassword + " -catalogConnect/domainName " + CatalogConnectString + " -catalogConnect/userName "  + CatalogUser  + " -catalogConnect/password " + CatalogPassword
                    elif CatalogConnectEnable.lower() == 'no' :
                                cmd = "qoperation execute -appName Oracle -af " + xmlfile + " -CS " + CommServerName +  " -clientName "  + ClientName + " -instanceName " + Instance + " -dataArchiveGroup/storagePolicyName " + DefaultStoragePolicy + " -commandLineStoragePolicy/storagePolicyName "+ CmdStoragePolicy + " -logBackupStoragePolicy/StoragePolicyName " + LogStoragePolicy + " -oracleHome " + "\'"  + OracleHome + "\'" + " -oracleUser/userName " + OracleUser + " -sqlConnect/userName " + ConnectUser + " -sqlConnect/domainName " + ConnectString +  " -sqlconnect/password " + ConnectPassword
                    (_retcode, _output) = cmdhelper.executeCommand(cmd)
                    _retval = str(_output)

                    #log.info("RETURN CODE IS" + _retcode)
                    _retval = str(_output)
                    log.info("OUTPUT IS" + _retval)

                    if _retval.find("<errorCode>0</errorCode>") >= 0:
                    #if (_retcode != 0):
                            log.info("OUTPUT IS" + _retval)
                            (_retcode, _query) = dbhelper.readAndReplaceVariables("GetOraMode.sql", (ClientName, Instance))
                            if(_retcode != 0):
                                return (False, _retval)

                            _data = dbhelper.getCVData(_query, CommServerName)
                            for i in range(len(_data)):
                                if _data[i][0] == "Oracle Version" :
                                    version = _data[i][1]
                                    break
                            try:
                                conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
                                curs = conn.cursor()
                                curs.execute('select version from v$instance')
                                row = curs.fetchone()
                                if row[0] != version:
                                    log.error("Version is not populated correctly in the GUI")
                                    return (False, _retval)
                                return (True, _retval)
                            except:
                                log.info("Database is in shutdown/started/mount mode or wrong oracle SID and instance created successfully")
                                shutdowncase = 'Done'
                                return (True, shutdowncase)

                    else:
                            log.error("User may not given correct values for instance properties. Please check")
                            return (False, _retval)

        except:
            log.exception("Exception raised at createInstance")
            return (None, _retval)


def getOracleHomePathfromWindows(Instance, ClientHostName, RemoteMachineUserName, RemoteMachinePassword):
    """
        Returns Oracle Home path on success, False on unhandled exception
    """
    log = loghelper.getLog()
    log.info("Getting the Oracle Home Path")
    OracleService = "OracleService" + Instance.upper()
    RemoteMachineUserName = RemoteMachineUserName.split('\\')
    UserName = RemoteMachineUserName[1]
    Domain = RemoteMachineUserName[0]

    try:
        retcode, entryTupleList = Registryhelper.getKeyEntries(AutomationConstants.ORACLE_HOME_ROOT + "\\" + OracleService,_winreg.HKEY_LOCAL_MACHINE, ClientHostName, UserName, RemoteMachinePassword, Domain)
        for i in entryTupleList:
            if i[0] == "ImagePath":
                path = i[1]
                break

        path = path.split('\\bin')
        log.info("Oracle Home Path is  :%s" % path)
        return str(path[0])

    except:
        log.exception("Exception Occured while getting registry entries")
        return None

def getOracleHomePath(Instance, ClientHostName, RemoteMachineUserName, RemoteMachinePassword, OSType):
    """
    Accordong to OSTYPE i will call correxponding funtion and resturns home path
    """
    log = loghelper.getLog()
    try:
        if OSType.lower() == 'windows':
            path = getOracleHomePathfromWindows(Instance, ClientHostName, RemoteMachineUserName, RemoteMachinePassword)
        else:
            path = UnixHelper.getOracleHomePathfromUnix(Instance, ClientHostName, RemoteMachineUserName, RemoteMachinePassword)
        return path
    except:
        log.exception("Exception raised in getOracleHomePath")
        return None

def isBackupDoneWithEncryption(FileName, jobId):
    """
    FileName: Full Path of ORASBT.log file.
    jobId: jobId for which we want to check encryption used or not
    """
    log = loghelper.getLog()
    try:
        '''
        #logic was working till B48. After that ORASBT log file format got changed, so we also had to change
        #validation logic

        Encryptiontype=('Blowfish', '3-DES', 'Serpent', 'TwoFish', 'AES')
        if os.path.isfile(FileName):
            for EncryptType in Encryptiontype:
                fd = open(FileName,"rU")
                for line in fd:
                    if line.find(EncryptType) >=0 and line.find(str(jobId)) >=0:
                        fd.close()
                        log.info("Found [" +EncryptType+ "] in file [" +FileName+ "]")
                        log.info("matching line is" +line)
                        return(0, True)
                fd.close()
                log.info("Encryption type [" +EncryptType+ "] not found in file [" +FileName+ "]")

            return(1, "No out of above 5 Encryption type found")
        '''
        if os.path.isfile(FileName):
            fd = open(FileName,"rU")
            for line in fd:
                if line.find("encryptionWhere [3]") >=0 and line.find(str(jobId)) >=0:
                    fd.close()
                    log.info("Found [encryptionWhere [3]] in file [" +FileName+ "]")
                    log.info("matching line is" +line)
                    return(0, True)
            fd.close()
            log.info(" encryptionWhere [3] not found in file [" +FileName+ "]")
            return(1, "Not found")
        else:
            log.error("File [" +FileName+ "] not found")
            return(2, "File not Found")
    except:
        log.exception("Exception raised in FindReplacePttrnInFile")
        return(-1,False)


def FindReplacePttrnInFile(FileName, FindPattern, ReplacePattern):
    """
    """
    log = loghelper.getLog()
    try:
        log.info("Trying to replace pattern "+FindPattern+ " with " +ReplacePattern+ \
                 " in file " +FileName)
        if os.path.isfile(FileName):
            TmpFileFD=open("tmpFile",'w')
            fd=open(FileName)
            for ln in fd.readlines():
                TmpFileFD.write(re.sub(FindPattern, ReplacePattern, ln))
            TmpFileFD.close()
            fd.close()
            os.remove(FileName)
            os.rename("tmpFile",FileName)
            log.info("Successfully replaced pattern all over in file " +FileName)
            return(0, True)
        else:
            log.error(FileName + " not found")
            return(1, False)
    except:
        log.exception("Exception raised in FindReplacePttrnInFile")
        return(-1,False)


def getCommonDirPath():
    """
    returns directory location which will be used to store files getting created during runtime of TC
    """
    log = loghelper.getLog()
    try:
        cv_cwd = loghelper.getLogDirectoryPath()
        dirname = "\\OracleCommon"
        if not os.path.isdir(cv_cwd + dirname):
            os.mkdir(cv_cwd + dirname)
        cv_cwd += dirname + "\\";
        log.info("Retrieved OracleCommon directory path successfully")
        return (0, cv_cwd);
    except:
        log.exception("Exception raised at getCommonDirPath")
        return (-1, False)

def removeCommonDir():
    """
    """
    log = loghelper.getLog()
    try:
        (_retCode, dirToDel) = getCommonDirPath()
        if os.path.isdir(dirToDel):
            Win_FS_Util.deleteDir(dirToDel)
            log.info("OracleCommon directory under log directory deleted successfully")
        else:
            log.info("OracleCommon dir is not present under log file")
            return(0, True)
    except:
        log.exception("Exception raised at removeCommonDir")
        return(-1, False)
def RenameLogFiles(ConnectUser, ConnectPassword, ConnectString, destPath , OSType):
    """
    This function renames online logfiles to destpath
    """
    log = loghelper.getLog()
    try:
        log.info("Trying to get log file path...")
        (retCode, retString, conn, cursor) = connectOracle(ConnectUser, ConnectPassword, ConnectString)
        if retCode == -1:
            log.error("OperationalError: ORA-01034: ORACLE not available")
            return (-1, False)

        #curs.execute("select status from v$instance")
        cursor.execute("select member from v$logfile")

        rows=cursor.fetchall()
        count=cursor.rowcount
        log.info( "#-records:", count)
        for i in range(0, count):
            log.info(" row: "+ rows[i][0]+" i is "+ str(i))

            firstrow=rows[i][0]
            row=rows[i][0]
            if OSType.lower() == "windows":
                var1 = row.split('\\')
            else:
                var1 = row.split(r'/')

            var1.reverse()

            if OSType.lower() == "windows":
                destLog = destPath + "\\" + var1[0]
            else:
                destLog = destPath + "/" + var1[0]

            #cmd[count]="alter database rename file '"+ firstrow[0] +"' to '"+ destLog+"'"
            #log.info("rename: " + cmd[count] + "count " + str(count))
            cmd="alter database rename file '"+ firstrow +"' to '"+ destLog+"'"
            log.info("rename: " + cmd + "count " + str(count))
            cursor.execute (cmd)

        return (0, True)
    except:
        log.exception("Exception raised at getLogFiles")
        return (-1, False)


def GetDatabaseState(ConnectUser, ConnectPassword, ConnectString):
    """
    """
    log = loghelper.getLog()
    try:
        log.info("Trying to retrieve DB state...")
        (retCode, retString, conn, curs) = connectOracle(ConnectUser, ConnectPassword, ConnectString)
        if retCode == -1:
            log.error("OperationalError: ORA-01034: ORACLE not available")
            return (1, False)
        curs.execute('select status from v$instance')
        row = curs.fetchone()
        return (0, row[0])
    except:
        log.exception("Exception raised inside GetdatabaseState")
        return (-1, False)


def OracleInstanceModify(_client, _dataagenttype, _instance, _dataStoragepolicy, _LogStoragepolicy, _CommServerName):
    """
    Modifies proporty of oracle instance.
    Main purpose is to modify LogStorage Policy for TC16542
    """
    log = loghelper.getLog()
    try:
        cmd = "qmodify instance -c " + _client +" -cs " + _CommServerName + " -a " + _dataagenttype + " -i " \
              + _instance + " -csp " + _dataStoragepolicy + " -lsp " + _LogStoragepolicy
        (_retcode, _output) = cmdhelper.executeCommand(cmd)
        _retval = str(_output)
        if _retval.find("Modified instance successfully") >= 0:
            log.info("Modified properties of instance " +_instance+ " successfully")
            return (0, True)
        else:
            log.error("Error while modifing properties of instance " +_instance)
            return (1, False)
    except:
        log.exception("Exception raised at OracleInstanceModify")
        return(-1, False)

def CreateOnDemandInstance(CSName, ClientName, DataAgentType):
    """
    """
    log = loghelper.getLog()
    try:
        cmd = "qcreate instance -cs " +CSName+ " -c " +ClientName+ " -a " +DataAgentType + \
              " -n \"On Demand Instance\""
        (retcode, retString) = cmdhelper.executeCommand(cmd)
        if str(retString).find("Created instance successfully") >=0:
            log.info("Created On Demand Instance successfully")
            return(0, True)
        else:
            log.info(retString)
            return (1, False)
    except:
        log.exception("Exception raised at CreateOnDemandInstance")
        return(-1, False)


def CreateOracleSubclient(CommServerName,ClientName,Instance,SubClientName,DefaultStoragePolicy):
    """
        creates a subClient and returns a boolean value indication status of creation
        Added by Shankar Vullupala.
    """
    log = loghelper.getLog()
    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\create_subclient_template.xml")
    try:
        cmd = "qoperation execute -af " + xmlfile + " -CS " + CommServerName + " -appName 'Oracle' -clientName " + ClientName + " -instanceName " + Instance + " -subClientName " + SubClientName + " -dataBackupStoragePolicy/storagePolicyName " + DefaultStoragePolicy

        (retcode, retval) = cmdhelper.executeCommand(cmd)
        print retcode
        if retcode !=0 :
            log.info("SubClient " + SubClientName + " Creation Failed --> " + Instance)
            return False
        else:
            log.info("SubClient " + SubClientName + " Creation Succeded --> " + Instance)
            return True
    except:
        log.exception("Exception raised at createRACSubclient")
        return False


"""
End Function to add RAC Pseudo Client and Oracle RAC Nodes
End Section - Added by Diptiman Basak on 28th Feb 2014 for Oracle RAC Automation.
"""

def recoverDatabase(commonDirPath, ConnectUser, ConnectPassword, ConnectString):
    """
    """
    log = loghelper.getLog()
    try:
        fd=open(commonDirPath+'recoverDB.sql', 'w')
        fd.write('recover database;\n')
        fd.write('recover database;\n')
        #fd.write('alter database open;\n')
        fd.write('exit;\n')
        fd.close()
        log.info("created recoverDB file successfully")
    except:
        log.error("failed to create recoverDB.sql with content as recover databse command")

    return (cmdhelper.executeCommand("sqlplus " +ConnectUser+"/"+ConnectPassword+"@"
                                     +ConnectString+" as sysdba @\"" + commonDirPath + "recoverDB.sql\""))


def startupDatabase(commonDirPath, ConnectUser, ConnectPassword, ConnectString):
    """
    """
    log = loghelper.getLog()
    try:
        fd=open(commonDirPath+'startDB.sql', 'w')
        fd.write('startup;\n')
        fd.write('exit;\n')
        fd.close()
        log.info("created startup file successfully")
    except:
        log.error("failed to create startDB.sql with content as recover databse command")

    return (cmdhelper.executeCommand("sqlplus " +ConnectUser+"/"+ConnectPassword+"@"
                                     +ConnectString+" as sysdba @\"" + commonDirPath + "recoverDB.sql\""))


def gracefulDBShutDown(commonDirPath, ConnectUser, ConnectPassword, ConnectString):
    """
    graceful shut down is must for TC 5055
    """
    log = loghelper.getLog()
    try:
        #create file contaning shutdown command
        fd=open(commonDirPath+'DBShutDown.sql','w')
        fd.write('shutdown immediate;\n')
        fd.write('exit;\n')
        fd.close()
    except:
        log.error("failed to create shutdown file")
    return (cmdhelper.executeCommand("sqlplus " +ConnectUser + "/" + ConnectPassword + "@"+ConnectString + " as sysdba @\"" + commonDirPath + "DBShutDown.sql\""))



def checkArchiveLogMode(conn, curs, ConnectUser, ConnectPassword, ConnectString, flag=True):
    """
    This function checks archive log mode and takes action specified by user
    """
    log = loghelper.getLog()
    (retCode, commonDirPath) = getCommonDirPath()
    curs.execute('select archiver from v$instance')
    row = curs.fetchone()

    if flag == True:
        if row[0] != "STARTED":
            try:
                #conn.shutdown(mode=cx_Oracle.DBSHUTDOWN_ABORT)
                gracefulDBShutDown(commonDirPath, ConnectUser, ConnectPassword, ConnectString)
                cmd = "sqlplus " +ConnectUser + "/" + ConnectPassword + "@"+ConnectString + " as sysdba @\"" + commonDirPath + "stop.sql\""
                os.system(cmd)
                #conn.close()
                conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
                curs = conn.cursor()
                #curs.execute('recover database; alter database archivelog')
                (retCode, retString) = recoverDatabase(commonDirPath, ConnectUser, ConnectPassword, ConnectString)
                if retCode != 1:
                    log.info("recovered database successfully")
                curs.execute('alter database archivelog')
                curs.execute('alter database open')
                log.info("Successfully enabled archive log mode")
                #return(0, True)
                return(0, curs)
            except:
                log.exception("Failed to enable archive log mode")
                return(1, False)
        else:
            log.info("archive log mode is already in enabled mode")
            return(0, curs)
    else:
        "user wants to set archivelog to disabled mode"
        if row[0] == "STARTED":
            try:
                #conn.shutdown(mode=cx_Oracle.DBSHUTDOWN_ABORT)
                gracefulDBShutDown(commonDirPath, ConnectUser, ConnectPassword, ConnectString)
                cmd = "sqlplus " +ConnectUser + "/" + ConnectPassword + "@"+ConnectString + " as sysdba @\"" + commonDirPath + "stop.sql\""
                os.system(cmd)
                conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
                curs = conn.cursor()
                (retCode, retString) = recoverDatabase(commonDirPath, ConnectUser, ConnectPassword, ConnectString)
                if retCode != 1:
                    log.info("recovered database successfully")
                curs.execute('alter database noarchivelog')
                curs.execute('alter database open')
                log.info("Successfully disabled archive log mode")
                return(0, curs)
            except:
                log.exception("Failed to disable archive log mode")
                return(1, False)
        else:
            log.info("archive log mode is already in disabled mode")
            return(0, curs)


def createRMANTxtFile():
    """
    """
    log = loghelper.getLog()
    try:
        (_retCode, CommonDirPath) = getCommonDirPath()
        fp=open(CommonDirPath+'rman.txt','w')
        fp.write('list backupset summary;')
        fp.close()
        log.info("Created RMANtxt file successfully")
        return(0, True)
    except:
        log.exception("Exception raised at createRMANTxtFile")
        return(-1, False)


def createStopSqlFile():
    """
    """
    log = loghelper.getLog()
    try:
        (_retCode, CommonDirPath) = getCommonDirPath()
        fp=open(CommonDirPath+'stop.sql','w')
        fp.write('startup nomount;\n')
        fp.write('alter database mount;\n')
        fp.write('exit;\n')
        fp.close()
        log.info("Created stopsql file successfully")
        return(0, True)
    except:
        log.exception("INside createStopSqlFile")
        return(-1, False)


def createStartupSqlFile(PFile):
    """
    create startup sql file with Specifynig PFile
    """
    log = loghelper.getLog()
    try:
        (_retCode, CommonDirPath) = getCommonDirPath()
        fp=open(CommonDirPath+'startup.sql','w')
        script = "startup nomount pfile = " + PFile + ";\n"
        fp.write(script)
        fp.write('exit;\n')
        fp.close()
        log.info("Created startupsql file successfully")
        return(0, True)
    except:
        log.exception("INside createStartupSqlFile")
        return(-1, False)

def createStartupOpenSqlFile():
    """
    startup command
    """
    log = loghelper.getLog()
    try:
        (_retCode, CommonDirPath) = getCommonDirPath()
        fp=open(CommonDirPath+'startupopen.sql','w')
        script = "startup;\n"
        fp.write(script)
        fp.write('exit;\n')
        fp.close()
        log.info("Created startupopensql file successfully")
        return(0, True)
    except:
        log.exception("INside createStartupSqlFile")
        return(-1, False)


def createDBMountSqlFile():
    """
    shutdown and put database in mount mode(used for pre/post scripts)
    """
    log = loghelper.getLog()
    try:
        (_retCode, CommonDirPath) = getCommonDirPath()
        fp=open(CommonDirPath+'dbmount.sql','w')
        fp.write('shutdown immediate;\n')
        fp.write('startup nomount;\n')
        fp.write('alter database mount;\n')
        fp.write('exit;\n')
        fp.close()
        log.info("Created dbmountsql file successfully")
        return(0, True)
    except:
        log.exception("INside createDBMountSqlFile")
        return(-1, False)

def createDBOpenSqlFile():
    """
    Put database in open mode.(used for pre/post scripts)
    """
    log = loghelper.getLog()
    try:
        (_retCode, CommonDirPath) = getCommonDirPath()
        fp=open(CommonDirPath+'dbopen.sql','w')
        script = "alter database open;\n"
        fp.write(script)
        fp.write('exit;\n')
        fp.close()
        log.info("Created DBopensql file successfully")
        return(0, True)
    except:
        log.exception("INside createDBOpenSqlFile")
        return(-1, False)



def createGetLogSeqFile(RMANcommand, filename):
    """
    """
    log = loghelper.getLog()
    try:
        (_retCode, CommonDirPath) = getCommonDirPath()
        fp=open(CommonDirPath+filename,'w')
        fp.write(RMANcommand)
        fp.close()
        log.info("Created " +filename+ " file successfully")
        return(0, True)
    except:
        log.exception("Exception raised at creategetlogseqFile")
        return(-1, False)


def create_backupsetTAG_file(CommonDirPath, tag):
    """
    """
    log = loghelper.getLog()
    try:
        fp=open(CommonDirPath+'backupsetTAG.txt','w')
        fp.write('list backupset tag ' +tag+ ';')
        fp.close()
        log.info("Created  backupsetTAG file successfully")
        return(0, True)
    except:
        log.exception("Exception raised at create_backupset_TAG_file")
        return(-1, False)

def getDBStatusByTag(ConnectUser, ConnectPassword, ConnectString, commonDirPath):
    """
    only used in  TC29829
    """
    log = loghelper.getLog()
    try:
        flag=False
        cmd = "rman target " + ConnectUser + "/" + ConnectPassword+"@"+ConnectString+" \"nocatalog @\'"+commonDirPath + "backupsetTAG.txt\'" + " log=\'" +commonDirPath +"listBackupsetTAG.log\'\""
        os.system(cmd)
        fp = open(commonDirPath+'listBackupsetTAG.log','r')
        for line in fp.readlines():
            if line.find("Status: AVAILABLE") >=0:
                flag=True
                break
            else:
                continue
        fp.close()
        if flag==True:
            log.info(" Successfullly retrieved backed up DB status which is in AVAILABLE status. good to go ahead")
            return (0, True)
        else:
            log.error("No backup found in AVAILABLE STATE OR Backup have not done even once wtih del_backup tag")
            return (1,False)

    except:
        log.exception("Exception raised at getDBStatusByTag")
        return (-1, False)


def getRestoreTimeFrom1stBackup(ConnectUser, ConnectPassword, ConnectString):
    """
    TC 3511 requirement: Toget restore date and time from 1st backup
        1. create rman file with proper commands
        2. create bat file which will contain commands to
            1. set date in proper format
            2. rman target command
        3. open log file which got created after bat file execution
        4. parse date & time
    """
    log = loghelper.getLog()
    try:
        (retCode, commonDirPath) = getCommonDirPath()
        fd=open(commonDirPath+'getRestoreTime.txt','w')
        fd.write('list backup of archivelog all;\n')
        fd.write('exit;\n')
        fd.close()
        log.info("Created getRestoreTime.txt file successfullly to get restore time")

        fd=open(commonDirPath+'RestoreDateTime.bat','w')
        fd.write('set NLS_DATE_FORMAT=RRRR-MM-DD HH24:MI:SS;\n')
        cmd = "rman target " + ConnectUser + "/" + ConnectPassword+"@"+ConnectString+" \"nocatalog @\'"+commonDirPath + 'getRestoreTime.txt' +"\'" + " log=\'" +commonDirPath +"getRestoreTime.log\'\""
        fd.write(cmd)
        fd.close()

        log.info("created RestoreDateTime.bat file successfully")
        cmdhelper.executeCommand(commonDirPath+'RestoreDateTime.bat')
        log.info("executed RestoreDateTime.bat file successfully")

        Columnn=7; i=-3; j=-2
        while 1:
            if i == -7:
                fp.close()
                break
            fp = open(commonDirPath+'getRestoreTime.log','r')
            line = str(fp.readlines()[int(i):int(j)])
            log.info("line we got is " +str(line))
            sptLine = line.split()
            try:
                if type(int(sptLine[int(Columnn-1)])).__name__ == 'int':
                    fp.close()
                    break
            except:
                log.info("nothing serious")
            i-=1 ; j-=1
            fp.close()
        if i == -7:
            log.error("Could not get data and time of recent backup job")
            return (1, False)
        else:
            log.info("successfully retrieved data and time of backup job as" +str(sptLine[int(Columnn)]) + str(sptLine[int(Columnn+1)]))
            return (0, str(sptLine[int(Columnn)]) + " " + str(sptLine[int(Columnn+1)]))

    except:
        log.error("failed to create getRestoreTime.txt file and retrieve restoreDataAndTime")
        return(-1, False)



def GetArchiveLOGTime(curs):
    """
    This function -actually- gets the Log time
    """
    log = loghelper.getLog()
    try:
        #curs.execute("select status from v$instance")
        #curs.execute("CREATE OR REPLACE FUNCTION oracle_to_unix(in_date IN DATE) RETURN NUMBER IS BEGIN RETURN (in_date -TO_DATE('19700101','yyyymmdd'))*86400; END;\n/\n")

        #curs.execute("select TO_CHAR(completion_time, 'YYYY-MM-DD hh24:mi:ss')  from (select completion_time  from v$archived_log where SEQUENCE# > 0 order by first_time desc) where rownum = 1")
        curs.execute("select TO_CHAR(completion_time, 'MM/DD/YYYY HH24:MI:SS')  from (select completion_time  from v$archived_log where SEQUENCE# > 0 order by first_time desc) where rownum = 1")
        cmd="select TO_CHAR(completion_time, 'MM/DD/YYYY HH24:MI:SS')  from (select completion_time  from v$archived_log where SEQUENCE# > 0 order by first_time desc) where rownum = 1"
        log.info(cmd)
        row = curs.fetchone()
        log.info ("Archive log time " + str(row[0]))
        return (0, str(row[0]))
    except:
        log.error("Failed to get GetArchiveLOGTime")
        return(-1, False)

def GetBackupLOGTime(curs):
    """
    This function -actually- gets the Log sequence number from the database
    """
    log = loghelper.getLog()
    try:
        #curs.execute("select status from v$instance")
        #curs.execute("CREATE OR REPLACE FUNCTION oracle_to_unix(in_date IN DATE) RETURN NUMBER IS BEGIN RETURN (in_date -TO_DATE('19700101','yyyymmdd'))*86400; END;\n/\n")
        #curs.execute("select TO_CHAR(first_time, 'YYYY-MM-DD hh24:mi:ss') from (select first_time  from v$backup_redolog  order by first_time desc) where rownum = 1")
        curs.execute("select TO_CHAR(first_time, 'MM/DD/YYYY HH24:MI:SS') from (select first_time  from v$backup_redolog  order by first_time desc) where rownum = 1")
        cmd="select TO_CHAR(first_time, 'MM/DD/YYYY HH24:MI:SS') from (select first_time  from v$backup_redolog  order by first_time desc) where rownum = 1"
        log.info(cmd)
        row = curs.fetchone()
        #log.info(row)
        log.info ("Backup log time " + str(row[0]))
        return (0, str(row[0]))
    except:
        log.error("Failed to get GetBackupLOGTime")
        return(-1, False)

def GetBackupSCN(curs):
        """
        This function -actually- BAckup SCn number from the database
        """
        log = loghelper.getLog()
        try:
            #curs.execute("select status from v$instance")
            curs.execute("select TO_CHAR(NEXT_CHANGE#)  from  (select NEXT_CHANGE# from v$backup_redolog  order by first_time desc) where rownum = 1")
            cmd="select TO_CHAR(NEXT_CHANGE#)  from  (select NEXT_CHANGE# from v$backup_redolog  order by first_time desc) where rownum = 1"
            #curs.execute("select TO_CHAR(NEXT_CHANGE#)  from  (select NEXT_CHANGE# from v$backup_redolog  order by NEXT_CHANGE# desc) where rownum = 1")
            #cmd="select TO_CHAR(NEXT_CHANGE#)  from  (select NEXT_CHANGE# from v$backup_redolog  order by NEXT_CHANGE# desc) where rownum = 1"
            log.info(cmd)
            row = curs.fetchone()
            log.info ("BAckup SCN Number " + row[0])
            return (0, row[0])
        except:
            log.error("Failed to get GetBackupSCN")
            return(-1, False)

def GetBackupLSN(curs):
        """
        This function -actually- gets the log sequence number
        """
        log = loghelper.getLog()
        try:
            #curs.execute("select status from v$instance")
            #curs.execute("select TO_CHAR(SEQUENCE#) from (select SEQUENCE# from v$backup_redolog  order by first_time desc) where rownum = 1")
            curs.execute("select TO_CHAR(SEQUENCE#) from (select SEQUENCE# from v$backup_redolog  order by NEXT_CHANGE# desc) where rownum = 1")
            #cmd="select TO_CHAR(SEQUENCE#) from (select SEQUENCE# from v$backup_redolog  order by first_time desc) where rownum = 1"
            cmd="select TO_CHAR(SEQUENCE#) from (select SEQUENCE# from v$backup_redolog  order by NEXT_CHANGE# desc) where rownum = 1"
            log.info(cmd)
            #curs.execute("select SEQUENCE# from (select SEQUENCE# from v$backup_redolog  order by first_time desc) where rownum = 1;")
            row = curs.fetchone()
            log.info ("LSN Number " + row[0])
            return (0, row[0])
        except:
            log.error("Failed to get GetBackupLSN")
            return(-1, False)

def GetArchiveLSN(curs):
    """
    This function -actually- gets the Log sequence number from the database
    """
    log = loghelper.getLog()
    try:
        #curs.execute("select status from v$instance")
        curs.execute("select TO_CHAR(SEQUENCE#) from (select SEQUENCE# from v$archived_log  where  SEQUENCE# > 0 order by first_time desc) where rownum = 1")
        cmd="select TO_CHAR(SEQUENCE#) from (select SEQUENCE# from v$archived_log  where  SEQUENCE# > 0 order by first_time desc) where rownum = 1"
        log.info(cmd)
        row = curs.fetchone()
        log.info ("LSN Number " + row[0])
        return (0, row[0])
    except:
        log.error("Failed to get GetArchiveLSN")
        return(-1, False)

def GetArchiveSCN(curs):
    """
    This function -actually- gets the System change number
    """
    log = loghelper.getLog()
    try:
        #curs.execute("select status from v$instance")
        curs.execute("select TO_CHAR(NEXT_CHANGE#) from (select NEXT_CHANGE# from v$archived_log  WHERE NEXT_CHANGE# > 0 order by first_time desc) where rownum = 1")
        cmd="select TO_CHAR(NEXT_CHANGE#) from (select NEXT_CHANGE# from v$archived_log  WHERE NEXT_CHANGE# > 0 order by first_time desc) where rownum = 1"
        log.info(cmd)
        row = curs.fetchone()
        log.info ("Archive SCN Number " + row[0])
        return (0, row[0])
    except:
        log.error("Failed to get GetArchiveSCN")
        return(-1, False)

def GetArchiveDest(curs):
    """
    This function -actually- gets the archive log destinations from the database
    """
    log = loghelper.getLog()
    try:
        #curs.execute("select status from v$instance")
        curs.execute("select a.destination from v$archive_dest a where a.status='VALID';")
        row = curs.fetchone()
        log.info ("Archive Dest " + str(row[0]))
        return (0, str(row[0]))
    except:
        log.error("Failed to get GetArchiveDest")
        return(-1, False)

def Getdbid(curs):
    """
    This function gets the DBID from the Oracle database
    """
    log = loghelper.getLog()
    try:
        curs.execute("select TO_CHAR(DBID) from v$database")
        row = curs.fetchone()
        log.info ("The DBID number we got from the database " + row[0])
        return (0, row[0])
    except:
        log.error ("Failed to get DBID from database ")
        return(-1, False)

def GetSequenceNumber(ConnectUser, ConnectPassword, ConnectString, RMANfilename, Columnn):
    """
    Only used in TC5019,5028 to get archive log number, SCN respectively
    """
    log = loghelper.getLog()
    try:
        (retCode, commonDirPath) = getCommonDirPath()
        cmd = "rman target " + ConnectUser + "/" + ConnectPassword+"@"+ConnectString+" \"nocatalog @\'"+commonDirPath + RMANfilename +"\'" + " log=\'" +commonDirPath +"logsequence.txt\'\""
        os.system(cmd)
        fp = open(commonDirPath+'logsequence.txt','r')
        lastline = str(fp.readlines()[-3:-2])
        if lastline.find("specification does not match any archived log") >=0:
            log.info("No backup has been run yet with archive logs enabled and archive delete disabled in subclient properties")
            log.info("So we will be using 1 as starting log sequence number.")
            log.info("if restore fails then make sure list archivelog all command displaying log number")
            log.info("which means u have to run atleast 1 backup whose logs are present in DB")
            fp.close()
            return (1, 1)
        fp.close()
        i=-3; j=-2
        while 1:
            if i == -7:
                fp.close()
                break
            fp = open(commonDirPath+'logsequence.txt','r')
            line = str(fp.readlines()[int(i):int(j)])
            log.info("line we got is " +str(line))
            sptLine = line.split()
            try:
                if type(int(sptLine[int(Columnn)])).__name__ == 'int':
                    fp.close()
                    break
            except:
                log.info("nothing serious")
            i-=1 ; j-=1
            fp.close()
        if i == -7:
            log.error("Could not get log sequence number")
            return (1, False)
        else:
            log.info(" Successfullly retrieved log sequence number as " +str(sptLine[int(Columnn)]))
            return (0, sptLine[int(Columnn)])
    except:
        log.exception("Exception raised in GetSequenceNumber")
        return(-1, False)

def backupsetKey(ConnectUser, ConnectPassword, ConnectString):
    """
    """
    log = loghelper.getLog()
    try:
        (retCode, commonDirPath) = getCommonDirPath()
        cmd = "rman target " + ConnectUser + "/" + ConnectPassword+"@"+ConnectString+" \"nocatalog @\'"+commonDirPath + "rman.txt\'" + " log=\'" +commonDirPath +"log.txt\'\""
        os.system(cmd)
        fp = open(commonDirPath+'log.txt','r')
        bskeyline = str(fp.readlines()[-3:-2])
        #if bskeyline.find(">") < 0 or bskeyline.find("RMAN") < 0:
        if bskeyline.find("RMAN") < 0:
            bskeyline = bskeyline.split(' ')
            bskeyline[0] = bskeyline[0].replace("['","")
            bskey = int(bskeyline[0])
            fp.close()
            log.info("Successfullly retrieved BSkey")
            return (0, bskey)
        else:
            log.error("possible causes are:")
            log.error("1. This is fresh DB. You need to run atleast 1 backup.")
            log.error("2. TARGET database is not compatible with this version of RMAN.")
            return (1, False)
    except:
        log.exception("Exception raised at backupsetKey")
        return (1, False)

def backupRmanScriptValidation(srchPattern, filepath, EndPattern=None):
    """
    for windows below assignment is fine
    """
    log = loghelper.getLog()
    try:
        log.info("inside Backup rman script validation")
        log.info(srchPattern)
        log.info(filepath)
        log.info(EndPattern)

        bfound = 0
        fd = open(filepath, 'r')
        for line in fd.readlines():
            if line.find(srchPattern) >= 0:
                log.info("Found string " + str(srchPattern))
                bfound = 1
                break

        fd.close()
        #if line != "Recovery Manager complete.\n":
        if bfound == 1:
            log.info("Backup RMAN script is correct found pattern " +str(srchPattern))
            return(0, True)
        else:
            log.error("Pattern " +str(srchPattern)+ " is not found in the backup RMAN script")
            return(-1, False)
    except:
        log.exception("Unhandled exception in backupRMANScriptValidation \n" + str(sys.exc_info()) )
        return (1, False)

def restoreRmanScriptValidation(srchPattern, filepath, EndPattern=None):
    """

if srchPattern in str(line) use this or change the patteren to complete string [sql 'alter tablespace TS16539 online ';

like srchPattern ="[sql 'alter tablespace TS16539 online ';"
    """
    log = loghelper.getLog()
    try:
        fd = open(filepath, 'r')

        bfound=0
        log.info("restore RMAN script found pattern " + str(srchPattern))
        for line in fd.readlines():
            #log.info("restore RMAN script found pattern " +str(line))
            if srchPattern in str(line) >= 0:
                log.info("Found string " + str(srchPattern))
                bfound = 1
                break

        fd.close()
        log.info("What was found? " + str(bfound))
        if bfound == 1:
            log.info("restore RMAN script found pattern " + str(srchPattern))
            return(0, True)
        else:
            log.error("[" +str(srchPattern)+ "] is not generated in the restore RMAN script")
            return(-1, False)
    except:
        log.exception("Unhandled exception in restoreRMANScriptValidation \n" + str(sys.exc_info()) )
        return (-1, False)


def backupValidate(oldbskey,ConnectUser,ConnectPassword,ConnectString):
    """
    """
    log = loghelper.getLog()
    try:
        (_retCode, newbskey) = backupsetKey(ConnectUser, ConnectPassword, ConnectString)
        (_retCode, commonDirPath) = getCommonDirPath()
        fp = open(commonDirPath+'validate.txt','w')
        fp.write('run { allocate channel ch1 type \'sbt_tape\'; validate backupset ')
        while int(oldbskey) < int(newbskey) :
            oldbskey = oldbskey + 1
            fp.write(str(oldbskey)+" ")

        fp.write(";\n}\nexit;")
        fp.close()
        cmd = "rman target " +ConnectUser+"/"+ConnectPassword+"@"+ConnectString+" \"nocatalog @\'"+commonDirPath + "validate.txt\'" + " log=\'" +commonDirPath +"status.txt\'\""
        os.system(cmd)
        fp = open(commonDirPath+'status.txt','r')
        line = str(fp.readlines()[-1:])
        line = line.split(' ')
        fp.close()
        if line[0] == "['RMAN-06160:" :
            log.error("Backup is not succeeded")
            return (1, False)
        else:
            log.info("Backup is succeeded")
            return (0, True)
    except:
        log.exception("Exception while Running backupValidate function")
        return(-1, False)

def restoreValidate(ConnectUser, ConnectPassword, ConnectString, DataFilePath=None):
    #print("In restore Validation fuction of oraclehelper library ")
    """
    """
    log = loghelper.getLog()
    try:
        '''
        ##FUTURE IMPROVEMENT:
        ##need to write a code which will check presence of restored datafile on client machine through ssh connecntion
        ##if that file is present, then we can say that restore completed successfully.
        '''
        conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
        curs = conn.cursor()
        if DataFilePath != None:
            curs.execute()

        curs.execute('select status from v$instance')
        row = curs.fetchone()
        if row[0] != "OPEN":
            log.error("Restore job did not complete successfully")
            return (1, False)
        else:
            log.info("Restore has completed successfully")
            return (0, True)
    except:
        log.exception("Exception while Running restoreValidate function")
        return(-1, False)



def getDatafile(curs, toCreate, OSType):
    """
    This function -actually- creates new datafile and returns its path so that we can mention it while creating tables
    """
    log = loghelper.getLog()
    try:
        #curs.execute("select status from v$instance")
        curs.execute("select name from v$datafile")
        row = curs.fetchone()
        firstrow=row[0]
        '''
        if OSType.lower() == "windows":
            var1 = row[0].split('\\')
            firstrow = firstrow.replace("\\", "\\\\")
        else:
            var1 = row[0].split(r'/')
            firstrow = firstrow.replace(r"/", r"//")
        '''

        i = len(firstrow) - 1
        while i >=0:
            if (firstrow[i] != "/" and firstrow[i] != "\\" ):
                DFile = firstrow[0:i]
            else:
                break
            i = i-1


        if toCreate == None:
            DFile = str(DFile)
        else:
            DFile = str(DFile)+ toCreate

        return (0, True, DFile)

    except:
        log.exception("Exception raised at getDatafile")
        return (-1, False, -1)

def restoreIncValidate(ConnectUser, ConnectPassword, ConnectString, tblSpace= None, tblName=None, rows=-1):
    """
    check restore of Incremental backup
    """
    log = loghelper.getLog()
    try:
        conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
        curs = conn.cursor()
        if int(rows) != -1:
            curs.execute('select count(*) from ' +tblName)
            row = curs.fetchone()
            if row[0] == int(rows):
                return(0, True)
        elif tblName != None and tblSpace != None:
            curs.execute('select table_name from dba_tables where tablespace_name=' +tblSpace)
            row=curs.fetchone()
            if row[0] >= 1:
                return(0, True)
            else:
                "Table backed up during INC nackup didn't restore properly"
                return(1,False)
        else:
            log.error("restoreIncValidate function didn't invoke properly")
            return(1, False)
    except:
        log.exception("Inside restoreIncValidate")
        return (-1, False)


def connectOracle(ConnectUser, ConnectPassword, ConnectString):
    """
    """
    log = loghelper.getLog()
    try:
        conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
        #conn = cx_Oracle.connect(str(ConnectUser), str(ConnectPassword), str(ConnectString), cx_Oracle.SYSDBA)
        curs = conn.cursor()
        log.info('Successfully connected to database: {0}'.format(ConnectString))
        return 0, True, conn, curs
    except Exception as e:
        log.exception("Exception raised at connectOracle: {0}".format(e))
        return -1, False, -1, -1


def create_table(curs, tablespace_name, table_prefix="CV_TAB_", user=None, number=1):
    """Creates tables and populates it with data"""
    log = loghelper.getLog()
    for count in range(1, number+1):
        table_name = table_prefix + '{:02}'.format(count)
        # Drop table if it exists
        if user is None:
            cmd = 'DROP TABLE {0} CASCADE CONSTRAINTS'.format(table_name)
        else:
            cmd = 'DROP TABLE {0}.{1} CASCADE CONSTRAINTS'.format(user, table_name)
        try:
            curs.execute(cmd)
        except Exception as str_err:
            log.warning('Unable to drop table: {0}'.format(str_err))
            pass
        # Create the actual table
        if user is None:
            cmd = 'CREATE TABLE {0} (ID NUMBER(5) PRIMARY KEY, NAME VARCHAR2(30)) TABLESPACE {1}'.format(table_name,
                                                                                                        tablespace_name)
        else:
            cmd = 'CREATE TABLE {0}.{1} (ID NUMBER(5) PRIMARY KEY, NAME VARCHAR2(30)) TABLESPACE {2}'.format(user,
                                                                                                            table_name,
                                                                                                            tablespace_name)
        log.info('Executing command: {0}'.format(cmd))
        try:
            curs.execute(cmd)
        except Exception as str_err:
            log.exception('Unable to execute command: {0}'.format(str_err))
            raise


def populate_data(curs, user=None, number=1):
    """Method to populate data in a table. Meant to be used in conjunction with create_table method"""
    log = loghelper.getLog()
    for count in range(1, number+1):
        table_name = "CV_TAB_" + '{:02}'.format(count)
        row_limit = 50
        log.info('Populating table: {}...'.format(table_name))
        for row_number in range(1, row_limit+1):
            if user is None:
                cmd = "INSERT INTO {0} VALUES ({1}, 'CV Automation - Test Case')".format(table_name,
                                                                                         str(row_number))
            else:
                cmd = "INSERT INTO {0}.{1} VALUES ({2}, 'CV Automation - Test Case')".format(user,
                                                                                              table_name,
                                                                                              str(row_number))
            try:
                log.info('Executing command: {0}...'.format(cmd))
                curs.execute(cmd)
            except Exception as str_err:
                log.warning('Unable to execute command: {0}'.format(str_err))
                pass
    curs.execute('commit')


def partial_snap_restore_validate(curs, table_prefix="CV_TAB_", user=None, number=1):
    """This is used to validate restore results for partial snaps"""
    log = loghelper.getLog()
    for count in range(1, number + 1):
        table_name = table_prefix + '{:02}'.format(count)
        row_limit = 50
        log.info('Validating table count for: {0}...'.format(table_name))
        if user is None:
            cmd = "SELECT COUNT(*) FROM {0}".format(table_name)
        else:
            cmd = "SELECT COUNT(*) FROM {0}.{1}".format(user, table_name)
        try:
            log.info('Executing command: {0}...'.format(cmd))
            count = curs.fetchall()[0][0]
            if not count == number:
                log.warning('Validation failed for table: {0}. Count: {1}'.format(table_name, count))
                return False
        except Exception as str_err:
            log.warning('Unable to execute command: {0}'.format(str_err))
            pass
    return True


def createTable(curs, firstrow, tblSpaceName, tblName, flagCreateTableSpace):
    """
    """
    log = loghelper.getLog()
    try:
        log.info("Creating table " +tblName +" under tablespace " +tblSpaceName)

        if flagCreateTableSpace == True:

            curs.execute("select tablespace_name from dba_tablespaces where tablespace_name = '" +tblSpaceName +"'")
            tablespaces = curs.fetchall()
            print tablespaces

            if str(tablespaces).find(tblSpaceName) >= 0:
                log.info( "drop tablespace " + tblSpaceName + " including contents and datafiles" )
                curs.execute("drop tablespace " + tblSpaceName + " including contents and datafiles" )
                log.info("tablesapce dropped")

            log.info( "create tablespace " + tblSpaceName + " datafile '" +firstrow + "' size 10M reuse")
            curs.execute("create tablespace " + tblSpaceName + " datafile '" +firstrow + "' size 10M reuse")
            log.info("Created tablespace sucessfully")
            print "tablespae created"

        log.info("select table_name from dba_tables where table_name = '" +tblName +"'")
        curs.execute("select table_name from dba_tables where table_name = '" +tblName +"'")
        tables = curs.fetchall()
        print tables

        if str(tables).find(tblName) >= 0:
            log.info("table dropped")
            print "drop table"
            curs.execute("drop table " +tblName)

        curs.execute("create table " + tblName + " (name varchar2(30), ID number)" + " tablespace " + tblSpaceName)
        #for count in range(1,10):
        #    curs.execute("insert into " +tblName + " values('commvault', 1)")

        for count in range(0, 10):
            curs.execute("insert into " +tblName + " values('" + tblName+str(count)+ "'," +  str(count) + ")")

        curs.execute("commit")

        log.info("Created table successfully")
        return 0, True
    except:
        return 1, False

def createtablewithBLOBandCLOB(curs, userName, firstrow, tblSpaceName, tblName, flagCreateTableSpace,numRows):
    """
    This function creates table with BLOB,CLOB colums
    """
    log = loghelper.getLog()
    try:
        log.info("Creating table " +tblName +" under tablespace " +tblSpaceName)
        if flagCreateTableSpace == True:
            curs.execute("create tablespace " + tblSpaceName + " datafile '" +firstrow + "' size 10M reuse")

        curs.execute("select table_name from dba_tables where tablespace_name = '" +tblSpaceName +"'" )
        tables = curs.fetchall()
        log.info(tables)
        if str(tables).find(tblName) >= 0:
            log.info("dropping table ")
            curs.execute("drop table " +tblName)
            log.info("table dropped sucessfully")
        log.info("creating table " + tblName + " with Blob and Clob columns")
        curs.execute("create table " + tblName + " (EMP_Name varchar2(30),EMP_VIDEO BFILE,RESUME CLOB,PICTURE BLOB)" )
        log.info("created " + tblName + " table with BLOB and Clob Columns")

        count=0
        for count in range(0,numRows):
            log.info("Inserting values into table "+ tblName )
            curs.execute("insert into "+ tblName + " values ('" + tblName+str(count)+"',NUlL,EMPTY_CLOB(),EMPTY_BLOB())")
            log.info("Sucessfully inserted values into table " + tblName)

        curs.execute("commit")
        log.info("Created table with BLOBandCLOB datatypes")
        return (0, True)
    except:
        return (1, False)

def CreateMultipleTablespaces(curs, firstrow, MoreTablespace, numTablespaces):
    """
    This function creates more than 50 tablespaces with each tablespace name with 25 characters long
    """
    log = loghelper.getLog()
    try:
        count=0
        for count in range(0,numTablespaces):
            createTablespace(curs, firstrow+str(count), MoreTablespace+str(count))
            log.info("Created tablespace " + MoreTablespace+str(count) + " successfully")
        return (0, True)
    except:
        log.exception("failed to create multiple tablespaces")
        return (1, False)

def CreateMultipleUsersWithTableRelation( curs, firstrow, tblSpaceName, userName, tblName, numTables):
    """
    This function creates 10 tablespaces,creates 10 user under the tablespace,Creates 2 tables under each user and inserts 10 rows in each tables for TC 35908
    """
    log = loghelper.getLog()
    try:

        count=0

        #create tablespace
        #create user
        #create two tables with relation under the user.
        for count in range(0,numTables):
            createTablespace(curs, firstrow+str(count), tblSpaceName+str(count))
            createUser(curs, userName+str(count), tblSpaceName+str(count))
            createTableWithRelation(curs, userName+str(count), firstrow, userName+str(count)+"."+ tblName+str(count), tblSpaceName+str(count), False,10)

        log.info("Created More tablespaces sucessfully")
        return (0, True)
    except:
        log.exception("Failed to create more createTableSpaces ")
        return (1, False)


def CleanupAuxillaryInstance(ClientHostName,RemoteMachineUserName,RemoteMachinePassword,OSType,StagingPath,jobid):
    """
    This function Checks that the Auxillary instance created during table restore is cleaned up after the table
    level restore was sucessfull
    """
    log = loghelper.getLog()

    try:
        if OSType.lower() == "windows":
            OS="windows"
            log.info("This is windows test machine ")
            path=StagingPath+"\\"+jobid
            log.info(path)
            cmd = " dir " + path
        else:
            OS="unix"
            log.info("This is unix test machine ")
            path=StagingPath+"/"+jobid
            log.info(path)
            cmd = " ls -lR " + path
            cmd = cmd + " | wc -l"

        sshConn = remoteconnection.Connection(ClientHostName, OS, username=RemoteMachineUserName, password=RemoteMachinePassword)
        log.info(cmd)
        (retCode,value)= sshConn.execute(cmd)
        sshConn.close()
        if retCode != 0:
            log.info("Auxillary instance created during tablelevel restore is cleaned up sucessfully")
            return (0, value)
        else:
            log.info("Auxillary instance created during tablelevel restore is not cleaned up sucessfully" + str(value))
            return (1, False)

    except:
        log.exception("Auxillary instance created during tablelevel restore is not cleaned up sucessfully")
        return (1, False)


def createTableWithconstraints(curs, userName, firstrow, tblName, tblSpaceName, flagCreateTableSpace,ConstraintName, numRows):
    """
    """
    log = loghelper.getLog()

    tblName1=tblName+str(1)
    tblName2=tblName+str(2)

    try:
        log.info("Creating table " +tblName +" under tablespace " +tblSpaceName)
        if flagCreateTableSpace == True:
            curs.execute("create tablespace " + tblSpaceName + " datafile '" +firstrow + "' size 10M reuse")

        curs.execute("create table " + tblName1 + " (person_name varchar2(30), ID1 number not null PRIMARY KEY)")

        curs.execute("create table " + tblName2 + " (name varchar2(50)not null PRIMARY KEY, ID1 number)")

        log.info ("created tables " + tblName1 + "," + tblName2 )

        count=0
        for count in range(0, numRows):
            curs.execute("insert into " +tblName1 + " values('" + tblName1+str(count)+ "'," +  str(count) + ")")

        log.info ("inserted values in "+ tblName1 )

        count=0
        for count in range(0, numRows):
            curs.execute("insert into " +tblName2 + " values('" + tblName2+str(count)+ "',"+ str(count) + ")")

        log.info ("inserted values in "+ tblName2)


        log.info("alter table " + tblName2 + " add constraint " + ConstraintName + "FOREIGN KEY (ID1) references " + tblName1)
        curs.execute("alter table " + tblName2 + " add constraint " + ConstraintName  + " FOREIGN KEY (ID1) references " + tblName1)

        log.info("altered table " +tblName2 + " sucessfully" )

        log.info ("added constraints sucessfully")

        log.info("Created table with constraints successfully")

        return (0, True)
    except:
        return (1, False)


def makedatafileonline(curs, firstrow):
    """
    This function keeps the datafile online for Tc 5023
    """
    log = loghelper.getLog()
    try:
        #log.info("To keep the datafile online.we need to startup db in mount mode")
        #curs.execute("shutdown abort")
        curs.execute("recover datafile '"+ firstrow + "'" )
        log.ino("Datfile was recovered sucessfully ")
        log.info("keeping the datafile online " +firstrow)
        curs.execute("alter database datafile '" + firstrow + "' online")
        log.info("datafile was sucessfully altered " + firstrow + " online")
        return(0, True)
    except:
        return(1, False)

def makedatafileoffline( curs, firstrow):
    """
    This function keeps the datafile offline used for TC 5023
    """
    log = loghelper.getLog()
    try:
        log.info("keeping the datafile offline " +firstrow)
        curs.execute("alter database datafile '" + firstrow + "' offline")
        log.info("datafile was sucessfully altered " + firstrow + " offline")
        return(0, True)
    except:
        return(1, False)

def createTableWithRelation(curs, userName, firstrow, tblName, tblSpaceName, flagCreateTableSpace, numRows):
    """
    """
    log = loghelper.getLog()

    tblName1=tblName+str(1)
    tblName2=tblName+str(2)
 
    try:
    
        if flagCreateTableSpace == True:
            (_retCode, _retString)=createTablespace(curs, firstrow,tblSpaceName)
            if _retCode != 0:
                log.info("Failed to Create Tablespaces")
                raise
            else:
                (_retCode, _retString) = createUser(curs, userName, tblSpaceName)
                if _retCode !=0:
                    log.error("Failed to create user name" + userName)
                    raise
            
        log.info("Creating table " +tblName1 +" under tablespace " +tblSpaceName)
        log.info("select table_name from dba_tables where table_name = '" +tblName1 +"'")
        curs.execute("select table_name from dba_tables where table_name = '" +tblName1 +"'")
        tables = curs.fetchall()
        print tables

        if str(tables).find(tblName1) >= 0:
            log.info("table dropped")
            print "drop table"
            curs.execute("drop table " +tblName1)

        curs.execute("create table " + tblName1 + " (name varchar2(30), ID number not null PRIMARY KEY)" + " tablespace " + tblSpaceName)
        #for count in range(1,10):
        #    curs.execute("insert into " +tblName + " values('commvault', 1)")

        
        count=0
        for count in range(0, numRows):
            curs.execute("insert into " +tblName1 + " values('" + tblName1+str(count)+ "'," +  str(count) + ")")

        log.info ("inserted values in "+ tblName1 )
        
        log.info("Creating table " +tblName2 +" under tablespace " +tblSpaceName)
        log.info("select table_name from dba_tables where table_name = '" +tblName2 +"'")
        curs.execute("select table_name from dba_tables where table_name = '" +tblName2 +"'")
        tables = curs.fetchall()
        print tables

        if str(tables).find(tblName2) >= 0:
            log.info("table dropped")
            print "drop table"
            curs.execute("drop table " +tblName2)

        curs.execute("create table " + tblName2 + " (name varchar2(30), ID number)" + " tablespace " + tblSpaceName)
        #for count in range(1,10):
        #    curs.execute("insert into " +tblName + " values('commvault', 1)")

        count=0
        for count in range(0, numRows):
            curs.execute("insert into " +tblName2 + " values('" + tblName2+str(count)+ "',"+ str(count) + ")")

        log.info ("inserted values in "+ tblName2)

        curs.execute("alter table " + tblName2 + " add constraint custom_name_fk FOREIGN KEY (ID) references " + tblName1)

        log.info ("add constraint")
        ##create grants
        #curs.execute("GRANT CREATE INDEX, SELECT ANY TABLE TO " + userName);

        ##create index
        #curs.execute("GRANT CREATE INDEX, SELECT ANY TABLE TO " + userName);

        ##create store procedure
        log.info("Created table with relations successfully")
        
        return (0, True)
    except:
        log.exception("Empty")
        return (1, False)


def restoreconstraintvalidate(ConnectUser, ConnectPassword, ConnectString, ConstraintName,table_name):
    """
    This function validates that the constraint we have created during backup exists after restore job
    """
    log = loghelper.getLog()
    try:
        conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
        curs = conn.cursor()
        curs.execute("select CONSTRAINT_NAME from dba_constraints where table_name='"+table_name+"'")
        cmd="select CONSTRAINT_NAME from dba_constraints where table_name='"+table_name+"'"
        log.info(cmd)
        Constraint = curs.fetchall()
        log.info("Constraints in the table are :" +str(Constraint))
        if curs.rowcount > 0:
            log.info("ConstraintName "+ ConstraintName +" exists in the dba_constraints.So table with constraints was restored sucessfully")
        return(0, True)
    except:
        return(1, False)

def restoretriggervalidate(ConnectUser, ConnectPassword, ConnectString,userName, TriggerName):
    """
    This function validates the   trigger that was dropped after backup exists after the restore job
    """
    log = loghelper.getLog()
    try:
        conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
        curs = conn.cursor()
        curs.execute("select TRIGGER_NAME,OWNER from dba_triggers where OWNER='"+userName +"' and TRIGGER_NAME ='" + TriggerName +"'")
        cmd="select TRIGGER_NAME,OWNER from dba_triggers where OWNER='"+userName +"' and TRIGGER_NAME ='" + TriggerName +"'"
        log.info(cmd)
        Trigger = curs.fetchall()
        log.info("The tiggers in the database are " + str(Trigger))
        if curs.rowcount > 0:
            log.info("Trigger name exists in the dba_triggers.So trigger was restored sucessfully")
            return(0, True)
        else:
            log.info("Trigger name not exists in the dba_triggers.So trigger was not restored sucessfully")
            return(1, False)

    except:
        return(1, False)

def restorestoredprocedurevalidate(ConnectUser, ConnectPassword, ConnectString, userName,procedureName):
    """
    """
    log = loghelper.getLog()
    try:
        conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
        curs = conn.cursor()
        cmd = "select OBJECT_NAME,PROCEDURE_NAME,OWNER from dba_procedures where OWNER='"+userName+"'"
        curs.execute(cmd)
        log.info(cmd)
        storedprocedure = curs.fetchall()
        log.info("The stored procedure in the database are :" +str(storedprocedure))
        #if str(storedprocedure).find(OBJECT_NAME) > 0:
        if curs.rowcount > 0:
            log.info("storedprocedure name exists in the dba_procedures.So procedure was restored sucessfully")
            return(0, True)
        else:
            log.info("storedprocedure name not exists in the dba_procedures.")
            return(1, False)

    except:
        return(1, False)


def createTablespace(curs, firstrow, tblSpaceName):
    """
    """
    log = loghelper.getLog()
    try:
        curs.execute("select tablespace_name from dba_tablespaces where tablespace_name = '" +tblSpaceName +"'")
        tablespaces = curs.fetchall()
        print tablespaces

        if str(tablespaces).find(tblSpaceName) >= 0:
                log.info( "drop tablespace " + tblSpaceName + " including contents and datafiles" )
                curs.execute("drop tablespace " + tblSpaceName + " including contents and datafiles" )
                log.info("tablespace dropped")
                log.info( "create tablespace " + tblSpaceName + " datafile '" +firstrow + "' size 10M reuse")
                curs.execute("create tablespace " + tblSpaceName + " datafile '" +firstrow + "' size 10M reuse")
                log.info("Created tablespace sucessfully")
                print "tablespace created"
        else:
                log.info( "create tablespace " + tblSpaceName + " datafile '" +firstrow + "' size 10M reuse")
                curs.execute("create tablespace " + tblSpaceName + " datafile '" +firstrow + "' size 10M reuse")
                log.info("Created tablespace sucessfully")
                print "tablespace created"
        return (0, True)
    except:
        return (1, False)



def createUser(curs, userName, tblSpaceName):
    """
    """
    log = loghelper.getLog()
    try:
        curs.execute("select USERNAME from dba_users")
        users = curs.fetchall()
        log.info(users)
        if str(users).find(userName) >= 0:
            log.info("Drop User " +userName)
            curs.execute("drop user " + userName + " cascade ")
            log.info("Dropped user sucessfully")
        log.info("create user " + userName + " identified by " +userName + " default tablespace " +tblSpaceName +" quota unlimited on " +tblSpaceName)
        try:
            curs.execute("create user " + userName + " identified by " +userName + " default tablespace " +tblSpaceName +" quota unlimited on " +tblSpaceName)
            log.info("Created user " + userName + "identified by" + userName + " Sucessfully " )
        except cx_Oracle.DatabaseError,msg:
            
            exit(1)
        curs.execute("grant connect, resource to " + userName)
        log.info("granted connect , resource to " + userName + "Sucessfully")
        #log.info("Granting sysdba privilages to " + userName)
        #curs.execute("grant sysdba to " + userName + " with admin option")
        #curs.execute("grant recovery_catalog_owner to " + userName)
        log.info("Created User successfully")
        return (0, True)
    except:
        log.exception("empty")
        return (1, False)

def dropUser(curs, userName):
    """
    """
    log = loghelper.getLog()
    try:
        log.info("Drop User " +userName)
        curs.execute("drop user " + userName + " cascade ")
        log.info("dropped User successfully")
        return (0, True)
    except:
        return (1, False)


def restoreTableValidate(ConnectUser, ConnectPassword, ConnectString, tblName, numrows):
    """
    """

    log = loghelper.getLog()
    try:
        conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
        curs = conn.cursor()

        if int(numrows) != -1:
            curs.execute('select * from ' +tblName)
            rows = curs.fetchall()
            if curs.rowcount != int(numrows):
                log.info ("Table backed didn't restore properly num of rows restored = " + str(curs.rowcount))
                return(1,False)
            else:

                log.info ("validating data")
                for i in range (0, curs.rowcount):
                    if rows[i][0] != tblName+str(i) or rows[i][1] != i :
                        log.info("Table is not restored correctly. data mismatch row: " + str(i) + " value " + tblName+str(i))
                        #return (1, False)
                    else:
                        log.info("Table data " + str(i) + " value " + tblName+str(i))

                log.info("Table is  restored correctly. num of rows: " +  str(curs.rowcount))
                return (0, True)

    except:
        log.info ("error during Table Validate")
        return (1, False)
def restoreBloBTableValidate(ConnectUser, ConnectPassword, ConnectString, tblName, numrows):
    """
    """

    log = loghelper.getLog()
    try:
        conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
        curs = conn.cursor()

        if int(numrows) != -1:
            curs.execute('select  EMP_NAME from ' +tblName)
            rows = curs.fetchall()
            log.info("Table Name " + tblName+ "num rows " + str(curs.rowcount))
            if curs.rowcount != int(numrows):
                log.info ("Table backed didn't restore properly num of rows restored = " + str(curs.rowcount))
                return(1,False)
            else:

                log.info ("validating data")
                log.info("Table is  restored correctly. num of rows: " +  str(curs.rowcount))
                return (0, True)

    except:
        log.info ("error during Table Validate")
        return (1, False)

def selectTablespaceForBackup(CSName, clientName, InstanceName, subclientName, tablespaceName, setBackup):
    """
    This function selects a tablespace for Backup.
    """
    log = loghelper.getLog()
    try:
        log.info ("selectTablespaceForBackup " + CSName + " " + clientName + " " + InstanceName + " " + subclientName + " " + tablespaceName)

        appTypeId = apptypehelper.getAppTypeID(AutomationConstants.ORACLE_DataAgentType, CSName, clientName)
        log.info("apptype id " + str(appTypeId))

        (instanceId, retVal)=instancehelper.getInstanceID(CSName, clientName, appTypeId, InstanceName)
        if retVal != True:
            return -1
        else:
            log.info ("got Instance id " + str(instanceId))

        subclientId=subclienthelper.getSCID( clientName, subclientName, CSName, AutomationConstants.ORACLE_DataAgentType, InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME)
        log.info ("got subclient id " + str(subclientId))

        #set tablespace, instance id and subclient id
        (retcode, query) = dbhelper.readAndReplaceVariables("SetOraTablespaceBackup.sql", (tablespaceName, instanceId, subclientId, setBackup))
        if(retcode != 0):
            return (retcode,false)
        else:
            log.info ("query "+ str(query))
            _retval=dbhelper.executeCVQuery(query, CSName)
            log.info ("query is successful")
            return 0

    except:
        log.info ("error in selectTablespaceForBackup")
        return -1

def createTableSpace(curs, datafile_loc, tblSpaceName, omf=False):
    """
    """
    log = loghelper.getLog()
    try:
        if omf:
            cmd = "create tablespace " + tblSpaceName
        else:
            cmd = "create tablespace " + tblSpaceName + " datafile '" + datafile_loc + "' size 10M"
        curs.execute(cmd)
        return 0, True
    except Exception as str_err:
        log.exception("Failed to create only tablespoace inside createTableSpace & not createTable: {}".format(str_err))
        return 1, False


#def deleteTable(curs, tblName):
#
#    curs.execute("drop table " +tblName)
#
#def deleteTableSpace(curs, tblSpaceName):
#    curs.execute("drop tablespace" +tblSpaceName)


#function to get control file backup file name for given jobid

def createlistincarnationfile(CommonDirPath ):
    """
    """
    log = loghelper.getLog()
    try:
        fp=open(CommonDirPath+'listincarnation.txt','w')
        fp.write('list incarnation ;')
        fp.close()
        log.info("Created  listincarnation file successfully")
        return(0, True)
    except:
        log.exception("Exception raised at createlistincarnation file")
        return(-1, False)

def GetParenttIncarnation(ConnectUser, ConnectPassword, ConnectString, commonDirPath):
    """
    This function get the PARENT incarnation value
    """
    log = loghelper.getLog()
    try:
        falg=False
        cmd = "rman target " + ConnectUser + "/" + ConnectPassword+"@"+ ConnectString+" @\'"+commonDirPath + "listincarnation.txt\'" + " log=\'" +commonDirPath +"listincarnation.log\'\""
        log.info(cmd)
        os.system(cmd)
        fp = open(commonDirPath+'listincarnation.log','r')
        first = fp.readlines()
        fp = first[len(first)-1]
        for line in fp.readlines():
            if line.find("PARENT") >=0:
                flag=True
                break
            else:
                continue
        fp.close()
        if flag==True:
            log.info(" Successfullly retrieved the PARENT incarnation with Status: PARENT . good to go ahead")
            return (0, True)
        else:
            log.error("Failed to get the incarnation with  Status: PARENT")
            return (1,False)

    except:
        log.exception("Exception raised while getting currentIncarnation ")
        return (-1, False)



def getAutoBackupControlFileHandle(CSName, jobId):
    """
    This function updates given Instance property
    """
    log = loghelper.getLog()
    try:
        log.info ("get archive file id for job " + str(jobId))
        (retCode, retVal) = dbhelper.readAndReplaceVariables("GetArchiveFileId.sql", (str(jobId), "%%"))
        if(retCode != 0):
            log.error("Error occured at while replacing values in sql file")
            return (False, "")

        retunCode = dbhelper.getCVData(retVal, CSName)
        return(True, retunCode[0][0])
    except:
        log.exception("Exception raised at getAutoBackupControlFileHandle")
        return (False, "")

def CreateStartupPFILEScript(ConnectUser,ConnectPassword, ConnectString, pureClientName, OSType, RemoteMachineUserName, RemoteMachinePassword,sshConn,ClientName,CommServerName):
    """
    """
    log = loghelper.getLog()
    (_retCode, CommonDirPath) = getCommonDirPath()

    try:

        if OSType.lower() == "windows":
            (retCode,retString)=clienthelper.getSimpanaInstalledDir(CommServerName, ClientName, 4)
            log.info("retString we got is " + str(retString)+ "\\Base")
            SimpanaBasePathCC = (str(retString)+ "\\Base")
            log.info("Simpana Base path we got is " + SimpanaBasePathCC)
            ###code to create ORACLETEMP under simpana BAse path and All the ODi related scripts are copied to that location and ODI###
            RemotePath = getRemoteTempPath(sshConn, SimpanaBasePathCC)
            log.info("remote path is" + str(RemotePath))
            pfilelocation=RemotePath+"\\pfile.ora"
        else:
            pfilelocation="/tmp/pfile.ora"

        log.info("Creating pfile from " + pfilelocation  )
        startupsql=CommonDirPath+'startup.sql'
        fp=open(startupsql,'w')

        script="Create pfile ='" + pfilelocation + "' from spfile;\n"
        fp.write(script)
        fp.write("shutdown immediate;\n")
        fp.write(script)
        fp.write('exit;\n')
        fp.close()

        cmd = "sqlplus " +ConnectUser+"/"+ConnectPassword+"@" +ConnectString+" as sysdba @\"" + startupsql
        log.info(cmd)
        os.system(cmd)

        log.info("Created pfile successfully")

        startupsql=os.path.join(CommonDirPath, "startup2.sql")
        log.info("startupsql " + startupsql)

        log.info(" pureClientName = " + pureClientName + " username " + RemoteMachineUserName)
        localPfilePath = os.path.join(CommonDirPath, "pfile.ora")

        if OSType.lower() == "windows":
            retCode = remoteconnection.getFromRemoteMachine(pureClientName, pfilelocation, localPfilePath, RemoteMachineUserName, RemoteMachinePassword)
            if str(retCode) != False:
                log.info("successfully copied pfile from Windows client machine " + pfilelocation)
            else:
                log.error("Failed to copy pfile from windows client machine")
                return (1, False)

        else:
            sshConn=remoteconnection.Connection(pureClientName, OSType, username=RemoteMachineUserName, password=RemoteMachinePassword)
            sshConn.get(pfilelocation,localPfilePath)

        #localPfilePath = os.path.join(CommonDirPath, "pfile.ora")

        fp=open(startupsql,'w')
        fp.write("startup pfile='" + localPfilePath + "' nomount;\n")
        fp.write('exit;\n')
        fp.close()

        cmd = "sqlplus " +ConnectUser+"/"+ConnectPassword+"@" +ConnectString+" as sysdba @\"" + startupsql
        log.info(cmd)
        os.system(cmd)

        log.info("Startup db using pfile successfully")

        return(0, True)
    except:
        log.exception("Exception raised at CreateStartupPFILEScript")
        return(-1, False)


def UpdateTNSNamesOraFile_New(tns_file, host, connect_identifier, service_name=None, port_number=1521):
    """
        Method to update TNS Names File with the Oracle Connect Identifier
    """
    if not os.path.isfile(tns_file):
        raise ValueError('Invalid TNS File names path.')
    log = loghelper.getLog()
    pattern = 'SERVICE_NAME = ' + service_name.split('.')[0]
    try:
        if service_name is None:
            service_name = connect_identifier
        with open(tns_file) as fd:
            for line in fd:
                if pattern in line:
                    log.warning('The TNS entry for {} altready exists.'.format(connect_identifier))
                    return
        with open(tns_file, 'a') as fd:
            fd.write("\n\n" + connect_identifier + " =\n")
            fd.write("\t(DESCRIPTION = \n")
            fd.write("\t\t(ADDRESS = (PROTOCOL = TCP)(HOST = " + host + ")(PORT = " + str(port_number) + "))\n")
            fd.write("\t\t(CONNECT_DATA = \n\t\t\t(SERVER = DEDICATED)\n")
            fd.write("\t\t\t(SERVICE_NAME = " + service_name + ")\n")
            fd.write("\t\t)\n\t)")
    except Exception as str_err:
        raise


def UpdateTNSNamesOraFile(TNSNamesFilePath, ClientName, ConnectString, port_number=1521):
    """
    """
    log = loghelper.getLog()
    try:
        SrchPattern = ConnectString + " =\n"
        fd = open(TNSNamesFilePath, 'r')
        lst = fd.readlines()
        if SrchPattern in lst:
            log.info(ConnectString + " already found in " + TNSNamesFilePath)
            return 1, False
        else:
            fd.close()
            fd = open(TNSNamesFilePath, 'a')
            fd.write("\n\n" + ConnectString + " =\n")
            fd.write("\t(DESCRIPTION = \n")
            fd.write("\t\t(ADDRESS = (PROTOCOL = TCP)(HOST = " + str(ClientName) + ")(PORT = " + str(port_number) + "))\n")
            fd.write("\t\t(CONNECT_DATA = \n\t\t\t(SERVER = DEDICATED)\n")
            fd.write("\t\t\t(SERVICE_NAME = " +ConnectString +")\n")
            fd.write("\t\t)\n\t)")
            fd.close()
            log.info("Updated " +TNSNamesFilePath +" successfully")
        return 0, True
    except Exception as str_err:
        log.exception("Inside UpdateTNSNamesOraFile: {}".format(str_err))
        return -1, False

def GetjobResultFromUnixMachine(jobId, LocalResultFileName, pureClientName, \
                                RemoteMachineUserName, RemoteMachinePassword, CommServerName, ClientName):
    """
    Get backup.out file from jobresults directory on UNIX client machine
    """
    log = loghelper.getLog()
    (_retCode, commDir) = getCommonDirPath()
    try:
        sshConn=remoteconnection.Connection(pureClientName, OS="unix", username=RemoteMachineUserName, password=RemoteMachinePassword)

        (retCode, jobResultDirPath) = clienthelper.getJobResultsFolder(CommServerName, ClientName)
        if retCode == 0:
            RemoteJobResultPath = jobResultDirPath+ r'/CV_JobResults/2/0/' +str(jobId)+ r"/" + LocalResultFileName
            log.info(RemoteJobResultPath)
        else:
            return(1, False)
        LocalJobResultPath = os.path.join(commDir, LocalResultFileName)
        sshConn.get(RemoteJobResultPath,LocalJobResultPath)
        log.info("successfully copied RMAN log file from unix client machine")
        return (0, LocalJobResultPath)
    except:
        log.exception("Exception raised at GetjobResultUnixMachine")
        return (1, False)

def GetLogFileAndPathForUnixAndWindows(jobId, FileNameToAppend, OSType,ClientHostName,ClientName, \
                                       RemoteMachineUserName, RemoteMachinePassword, CommServerName):
    """
    check for unix, windows(local, rmeote) platform and puul out log file accordingly
    """
    log = loghelper.getLog()
    (_retCode, commDir) = getCommonDirPath()
    pureClientName=ClientName.replace("_cn", "")
    pureClientName=pureClientName.replace("_CN", "")
    pureClientName = ClientHostName
    try:
        if OSType.lower() == "windows":
            log.info("this is windows test machine")
            #SimpanaInstalledPath=cmdhelper.getBasePath()
            (retCode, jobResultDirPath) = clienthelper.getJobResultsFolder(CommServerName, ClientName)
            if retCode == 0:
                RemoteJobResultPath = jobResultDirPath+r'\CV_JobResults\2\0\\'+str(jobId)+ r'\\' + FileNameToAppend
                log.info("RMAN log file is: " + str(RemoteJobResultPath))
                LocalJobResultPath = os.path.join(commDir, FileNameToAppend)
                if remoteconnection.getLocalHostName() != pureClientName:
                    retCode = remoteconnection.getFromRemoteMachine(pureClientName, RemoteJobResultPath, LocalJobResultPath, RemoteMachineUserName, RemoteMachinePassword)
                    if str(retCode) != False:
                        log.info("successfully copied RMAN log file from Windows client machine")
                        return (0, LocalJobResultPath)
                    else:
                        log.error("Failed to copy RMAN log file from windows client machine")
                        return (1, False)
                else:
                    log.info("seems we are running code from windows local machine which simpana client machine only")
                    return (0, RemoteJobResultPath)
            else:
                log.error("Not able to find simpana base path")
                return (1, False)
        else:
            log.info("this is linux test machine")
            (_retCode, _retString)=GetjobResultFromUnixMachine(jobId, FileNameToAppend, pureClientName, \
                                                                 RemoteMachineUserName, RemoteMachinePassword, CommServerName, ClientName)
            if _retCode == 1:
                log.error("Could not get jobResults from UNIX client machine")
                return (1, False)
            else:
                return (0, _retString)
    except:
        log.exception("Inside GetLogFileAndPathForUnixAndWindows")
        return (-1, False)



def getPFileFromUnixOrWindows(ClientName, UserName, Password, OSType, PFileLocation):

    """
    check for unix, windows(local, remote) platform and puul out pfile file accordingly
    """
    log = loghelper.getLog()
    (_retCode, commDir) = getCommonDirPath()
    try:
        if OSType.lower() == "windows":
            log.info("this is windows test machine")
            #SimpanaInstalledPath=cmdhelper.getBasePath()
            LocalPFilePath = os.path.join(commDir, "init.ora")
            if remoteconnection.getLocalHostName() != ClientName:
                retCode = remoteconnection.getFromRemoteMachine(ClientName, PFileLocation, LocalPFilePath, UserName, Password)
                if str(retCode) != False:
                    log.info("successfully copied PFile from Windows client machine")
                    return (0, LocalPFilePath)
                else:
                    log.error("Failed to copy PFile from windows client machine")
                    return (1, False)
            else:
                log.info("seems we are running code from windows local machine which simpana client machine only")
                return (0, PFileLocation)

        else:
            log.info("this is linux test machine")
            try:
                sshConn=remoteconnection.Connection(ClientName, OS="unix", username=UserName, password=Password)
                LocalPFilePath = os.path.join(commDir, "init.ora")
                #print "common dir is" + commDir
                #print "locatl file path" + LocalPFilePath
                sshConn.get(PFileLocation, LocalPFilePath )
                log.info("successfully copied PFile from unix client machine")
                return (0, LocalPFilePath)
            except:
                log.exception("Exception raised at GetPFileUnixMachine")
                return (1, False)

    except:
        log.exception("Inside GetPFileFromUnixOrWindows")
        return (-1, False)

def checkDatabaseMode(DuplicateDBConnectUser, DuplicateDBConnectPassword, DuplicateDBConnectString,\
                      DuplicateDBClientName, DuplicateDBClientUserName, DuplicateDBClientUserPassword,\
                      OSType, DuplicateDBPFileLocation):
    """
     Check database state and if database is not in started mode it will put in started mode
    """
    log = loghelper.getLog()
    try:
        (retCode, LocalPFilePath) = getPFileFromUnixOrWindows(DuplicateDBClientName, DuplicateDBClientUserName, DuplicateDBClientUserPassword, OSType, DuplicateDBPFileLocation)
        if retCode != 0:
            return False
        conn = cx_Oracle.connect(DuplicateDBConnectUser, DuplicateDBConnectPassword, DuplicateDBConnectString, cx_Oracle.SYSDBA)
        curs = conn.cursor()
        curs.execute('select status from v$instance')
        row = curs.fetchone()
        conn.shutdown(mode=cx_Oracle.DBSHUTDOWN_ABORT)
        '''
        if row[0] != "STARTED":
            log.info("Duplicate database is not in started mode, so putting database in started mode")
            conn.shutdown(mode=cx_Oracle.DBSHUTDOWN_ABORT)
        else:
            return LocalPFilePath
        '''
    except:
        log.info(" Duplicate database is in shutdown mode, need to put in startup mode")

    (retCode3, restString3) = createStartupSqlFile(LocalPFilePath)
    if retCode3 != 0 :
        return False

    (retCode, commonDirPath)= getCommonDirPath()
    cmd = "sqlplus " +DuplicateDBConnectUser+"/"+DuplicateDBConnectPassword+"@"+DuplicateDBConnectString \
                  +" as sysdba @\"" + commonDirPath + "startup.sql\""
    os.system(cmd)
    return LocalPFilePath

def restoreDuplicateDBValidate(DuplicateDBConnectUser, DuplicateDBConnectPassword, DuplicateDBConnectString, \
                               LocalPFilePath, ConnectUser, ConnectPassword, ConnectString):
    """
    Compares the datafile locations in the given PFile and from the duplicate databse after restore. Incase of same path structure on both source and destiantion it will check
    """
    log = loghelper.getLog()
    try:
        conn = cx_Oracle.connect(DuplicateDBConnectUser,DuplicateDBConnectPassword, DuplicateDBConnectString, cx_Oracle.SYSDBA)
        curs = conn.cursor()

        curs.execute('select status from v$instance')
        row = curs.fetchone()
        if row[0] != "OPEN":
            log.error("Restore job did not complete successfully")
            return (-1, False)
        else:
            log.info("Restore has completed successfully")
            log.info("Checking for datafile/tempfile/redolog fiel locations")
            curs.execute('select name from v$datafile')
            datafile = curs.fetchall()
            curs.execute('select name from v$tempfile')
            tempfile = curs.fetchall()
            curs.execute('select member from v$logfile')
            logfile = curs.fetchall()
            if os.path.isfile(LocalPFilePath):
                fd=open(LocalPFilePath,'r')
                count = 0
                for ln in fd.readlines():
                    if count ==2:
                        break
                    if ln.find("DB_FILE_NAME_CONVERT") >= 0 or ln.find("LOG_FILE_NAME_CONVERT") >= 0 :
                        count = count+1
                        ln = ln.replace(" ","")
                        ln = ln.replace(")",",")
                        ln = ln.split(',')
                        i = 1
                        while i < len(ln) :

                            if (str(datafile).find(ln[i]) >= 0):
                                log.info("datafile macthes" +ln[i])
                            elif (str(tempfile).find(ln[i]) >= 0):
                                log.info("tempfile macthes" +ln[i])
                            elif (str(logfile).find(ln[i]) >=0) :
                                log.info("logfile %s macthes" +ln[i])
                            else:
                                log.info("Datafile/tempfile/logfile is not matched" +ln[i])
                                return (-1, False)
                            i = i+2

            else:
                log.info("PFIle is not exist. Please cehcke")
                return (-1, False)
            if count == 2:
                log.info("all Datafiles and logfiles are matched")
                return (count,True)
            else :
                log.info("either DB_FILE_NAME_CONVERT or LOG_FILE_NAME_CONVERT OR Both may not preseent in PFILE. ")
                conn1 = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
                curs1 = conn1.cursor()
                curs1.execute("select name from v$datafile")
                sourcedatafiles = curs1.fetchall()
                curs1.execute("select member from v$logfile")
                sourcelogfiles = curs1.fetchall()

                curs.execute("select name from v$datafile")
                destdatafiles = curs.fetchone()
                curs.execute("select member from v$logfile")
                destlogfiles = curs.fetchone()

                print sourcelogfiles
                print destlogfiles

                if str(sourcedatafiles).find(str(destdatafiles)) >= 0:
                    log.info("Source and Destination datafile locations are matched")
                    if str(sourcelogfiles).find(str(destlogfiles)) >= 0:
                        log.info("Source and Destination log files are matched.")
                    else:
                        log.info("may be LOG_FILE_NAME CONVERT present in PFile")
                elif str(sourcelogfiles).find(str(destlogfiles)) >= 0:
                    log.info("Source and Destination log files are matched. for datafiles DB_FILE_NAME_CONVERT is present in PFile")
                else:
                        log.info("source and destiantion locations are not matched")
                        return(count, False)
                return (count, True)


    except:
        log.exception("Exception while Running restoreDuplicateDBValidate function")
        return (-1, False)

def getControlFileBackupPiece(CSName, jobId):
    """
    This function updates given Instance property
    """
    log = loghelper.getLog()
    try:
        log.info ("get archive files for job " + str(jobId))

        (retCode, retVal) = dbhelper.readAndReplaceVariables("GetArchFiles.sql", (str(jobId)))

        if retCode != 0:
            log.error("Error occured at while replacing values in sql file")
            return False

        backupPiece = dbhelper.getCVData(retVal, CSName)

        return (backupPiece[len(backupPiece)-1][0])
    except:
        log.exception("Exception raised at getAutoBackupControlFileHandle")
        return False


def getCommServerTimeZone(CSName):
    """
    This function updates given Instance property
    """
    log = loghelper.getLog()
    try:
        log.info ("get commserver time zone" + CSName)

        (retCode, retVal) = dbhelper.readAndReplaceVariables("GetCommServeTimeZone.sql", "" )

        if retCode != 0:
            log.error("Error occured at while replacing values in sql file")
            return False
        TimeZone = dbhelper.getCVData(retVal, CSName)
        TimeZone = str(TimeZone[0][0]).split(":", 2)
        return (TimeZone[2])

    except:
        log.exception("Exception raised at getAutoBackupControlFileHandle")
        return False

def getCommServerTimeZoneID(CSName):
    """
    This function gets the commserver time zone id from the CS database
    """
    log = loghelper.getLog()
    try:
        log.info ("get commserver time zone" + CSName)

        (retCode, retVal) = dbhelper.readAndReplaceVariables("GetCommServeTimeZoneID.sql", "" )

        if retCode != 0:
            log.error("Error occured at while replacing values in sql file")
            return False
        TimeZoneID = dbhelper.getCVData(retVal, CSName)
        TimeZoneID = str(TimeZoneID[0][0]).split(",")
        return (TimeZoneID[0])

    except:
        log.exception("Exception raised at getCommserverTimezoneID")
        return False

def getSimpanaInstanceName(ClientName, CSName):
    """
    This function updates given Instance property
    """
    log = loghelper.getLog()
    try:
        log.info ("get simpana instance name for client" + ClientName)

        (retCode, retVal) = dbhelper.readAndReplaceVariables("GetSimpanaInstanceName.sql", ClientName )

        if retCode != 0:
            log.error("Error occured at while replacing values in sql file")
            return False
        InstanceName = dbhelper.getCVData(retVal, CSName)
        return (InstanceName[0][0])

    except:
        log.exception("Exception raised at getAutoBackupControlFileHandle")
        return False

def getCommServerTimeZoneID(CSName):
    """
    This function gets the commserver time zone id from the CS database
    """
    log = loghelper.getLog()
    try:
        log.info ("get commserver time zone" + CSName)

        (retCode, retVal) = dbhelper.readAndReplaceVariables("GetCommServeTimeZoneID.sql", "" )

        if retCode != 0:
            log.error("Error occured at while replacing values in sql file")
            return False
        TimeZoneID = dbhelper.getCVData(retVal, CSName)
        TimeZoneID = str(TimeZoneID[0][0]).split(",")
        return (TimeZoneID[0])

    except:
        log.exception("Exception raised at getCommserverTimezoneID")
        return False

def getSimpanaInstanceName(ClientName, CSName):
    """
    This function updates given Instance property
    """
    log = loghelper.getLog()
    try:
        log.info ("get simpana instance name for client" + ClientName)

        (retCode, retVal) = dbhelper.readAndReplaceVariables("GetSimpanaInstanceName.sql", ClientName )

        if retCode != 0:
            log.error("Error occured at while replacing values in sql file")
            return False
        InstanceName = dbhelper.getCVData(retVal, CSName)
        return (InstanceName[0][0])

    except:
        log.exception("Exception raised at getAutoBackupControlFileHandle")
        return False

def getBackupEndTime1(ClientName, CommServerName, jobId):
    """
    This function updates given Instance property
    """
    log = loghelper.getLog()
    try:
        log.info ("get backup job end time " )

        cmd = "qoperation execscript -cs " + CommServerName + " -sn GetJobList.sql -si " + ClientName

        (_retcode, _output) = cmdhelper.executeCommand(cmd)
        print "backup time"
        val = str(_output[3])
        endTime = val.split()
        print endTime[8]

        TimeZone = getCommServerTimeZone(CommServerName)
        print TimeZone



        if retcode != 0:
            log.error("Error occured at while replacing values in sql file")
            return False


        return (TimeZone[2])
    except:
        log.exception("Exception raised at getAutoBackupControlFileHandle")
        return False


def getBackupEndTime2(CommServerName, jobId):
    """
    This function gets backup end time
    """
    log = loghelper.getLog()
    try:
        log.info ("get Backup End time" )

        (retCode, retVal) = dbhelper.readAndReplaceVariables("GetEndTime_BackupJob.sql",jobId )

        if retCode != 0:
            log.error("Error occured at while replacing values in sql file")
            return (1,False)

        TimeZone = dbhelper.getCVData(retVal, CommServerName)
        endTime = datetime.fromtimestamp(TimeZone[0][0])
        return (0,endTime)

    except:
        log.exception("Exception raised at getAutoBackupControlFileHandle")
        return (1,0)

def setAdvancedBackupArchiveLog(CSName, ClientName, InstanceName,subclientName,Enable):
	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML - DISABLE SKIP OFFLINE TABLESPACES")

		cmd = "qoperation execute -af " +xmlfile + " -appName Oracle -backupArchiveLog true -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName
		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setAdvancedBackupArchiveLog")
			return (True, _retval)
		else:
			log.error("Failed to setAdvancedBackupArchiveLog")
			return (False, _retval)
	except:
		log.exception("Exception raised at setAdvancedBackupArchiveLog")
		return (-1, False)

def setAdvancedArchiveLogDeleting(CSName, ClientName, InstanceName,subclientName,Enable):
    log = loghelper.getLog()

    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")


    try:
        log.info("Update Subclient Properties using XML - DISABLE SKIP OFFLINE TABLESPACES")

        cmd = "qoperation execute -af " + xmlfile + " -appName Oracle -archiveDelete true -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

        (_retcode, _output) = cmdhelper.executeCommand(cmd)
        _retval = str(_output)
        log.info("OUTPUT IS" + _retval)

        if _retval.find("<errorCode>0</errorCode>") >= 0:
            log.info("Successfully setAdvancedArchiveLogDeleting")
            return (True, _retval)
        else:
            log.error("Failed to setAdvancedArchiveLogDeleting")
            return (False, _retval)
    except:
        log.exception("Exception raised at setAdvancedArchiveLogDeleting")
        return (-1, False)

def setSubclientPropOracleBackupArchiveLogFlag(CSName, ClientName, InstanceName,subclientName,Enable):
    log = loghelper.getLog()

    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")


    try:
        log.info("Update Subclient Properties using XML - DISABLE SKIP OFFLINE TABLESPACES")

        cmd = "qoperation execute -af " + xmlfile + " -appName Oracle -backupArchiveLog true -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

        (_retcode, _output) = cmdhelper.executeCommand(cmd)
        _retval = str(_output)
        log.info("OUTPUT IS" + _retval)

        if _retval.find("<errorCode>0</errorCode>") >= 0:
            log.info("Successfully setSubclientPropOracleBackupArchiveLogFlag")
            return (True, _retval)
        else:
            log.error("Failed to setSubclientPropOracleBackupArchiveLogFlag")
            return (False, _retval)
    except:
        log.exception("Exception raised at setSubclientPropOracleBackupArchiveLogFlag")
        return (-1, False)

def setSubclientPropOracleDeleteArchiveLogFlag(CSName, ClientName, InstanceName,subclientName,Enable):

    log = loghelper.getLog()

    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

    try:
        log.info("Update Subclient Properties using XML - DISABLE SKIP OFFLINE TABLESPACES")

        cmd = "qoperation execute -af " + xmlfile + " -appName Oracle -archiveDelete true -clientName " + ClientName + " -instanceName " + InstanceName + " -subclientName " + subclientName

        (_retcode, _output) = cmdhelper.executeCommand(cmd)
        _retval = str(_output)
        log.info("OUTPUT IS" + _retval)

        if _retval.find("<errorCode>0</errorCode>") >= 0:
            log.info("Successfully setSubclientPropOracleDeleteArchiveLogFlag")
            return (True, _retval)
        else:
            log.error("Failed to setSubclientPropOracleDeleteArchiveLogFlag")
            return (False, _retval)
    except:
            log.exception("Exception raised at setSubclientPropOracleDeleteArchiveLogFlag")
            return (-1, False)

def setSubclientPropOracleBackupArchiveLogDestinations(CSName, ClientName, InstanceName,subclientName,Enable):
    log = loghelper.getLog()

    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")


    try:
        log.info("Update Subclient Properties using XML - DISABLE SKIP OFFLINE TABLESPACES")

        cmd = "qoperation execute -af " + xmlfile + " -appName Oracle -selectArchiveLogDestForBackup true -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

        (_retcode, _output) = cmdhelper.executeCommand(cmd)
        _retval = str(_output)
        log.info("OUTPUT IS" + _retval)

        if _retval.find("<errorCode>0</errorCode>") >= 0:
            log.info("Successfully setSubclientPropOracleBackupArchiveLogDestinations")
            return (True, _retval)
        else:
            log.error("Failed to setSubclientPropOracleBackupArchiveLogDestinations")
            return (False, _retval)
    except:
        log.exception("Exception raised at setSubclientPropOracleBackupArchiveLogDestinations")
        return (-1, False)

def setSubclientPropOracleDeleteArchiveLogDestinations(CSName, ClientName, InstanceName,subclientName,Enable):
    log = loghelper.getLog()

    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")


    try:
        log.info("Update Subclient Properties using XML - DISABLE SKIP OFFLINE TABLESPACES")

        cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -selectArchiveLogDestForDelete true -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

        (_retcode, _output) = cmdhelper.executeCommand(cmd)
        _retval = str(_output)
        log.info("OUTPUT IS" + _retval)

        if _retval.find("<errorCode>0</errorCode>") >= 0:
            log.info("Successfully setSubclientPropOracleDeleteArchiveLogDestinations")
            return (True, _retval)
        else:
            log.error("Failed to setSubclientPropOracleDeleteArchiveLogDestinations")
            return (False, _retval)
    except:
            log.exception("Exception raised at setSubclientPropOracleDeleteArchiveLogDestinations")
            return (-1, False)

def setSubclientPropLightsOutScriptON(CSName, ClientName, InstanceName,subclientName,Enable):
    log = loghelper.getLog()

    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")


    try:
        log.info("Update Subclient Properties using XML - ENABLE LIGHTS OUT SCRIPT")

        cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -lightsOutScript true -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

        (_retcode, _output) = cmdhelper.executeCommand(cmd)
        _retval = str(_output)
        log.info("OUTPUT IS" + _retval)

        if _retval.find("<errorCode>0</errorCode>") >= 0:
            log.info("Successfully setSubclientPropLightsOutScriptON")
            return (True, _retval)
        else:
            log.error("Failed to setSubclientPropLightsOutScriptON")
            return (False, _retval)
    except:
        log.exception("Exception raised at setSubclientPropLightsOutScriptON")
        return (-1, False)

def setSubclientPropBackupSpFile(CSName, ClientName, InstanceName,subclientName,Enable):

	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML - ENABLE/DISABLE SPFILE BACKUP")
		if Enable == 1:
			cmd = "qoperation execute -af " +xmlfile+" -appName Oracle -backupSPFile true -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName
		else:
			cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -backupSPFile false -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setSubclientPropBackupSpFile")
			return (True, _retval)
		else:
			log.error("Failed to setSubclientPropBackupSpFile")
			return (False, _retval)
	except:
		log.exception("Exception raised at setSubclientPropBackupSpFile")
		return (-1, False)

def setSubclientPropBackupControlFile(CSName, ClientName, InstanceName,subclientName,Enable):

	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML - ENABLE/DISABLE CONTROLFILE BACKUP")
		if Enable == 0:
			cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -backupControlFile false -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName
		else:
			cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -backupControlFile true -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setSubclientPropBackupControlFile")
			return (True, _retval)
		else:
			log.error("Failed to setSubclientPropBackupControlFile")
			return (False, _retval)
	except:
		log.exception("Exception raised at setSubclientPropBackupControlFile")
		return (-1, False)

def setSubclientPropBackupMode(CSName, ClientName, InstanceName, subclientName,Enable):
    return(subclienthelper.setSubClientProp(subclientName,"Oracle Backup Mode","7",str(Enable),CSName,ClientName, \
                                            AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
                                            InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))

def setSubclientPropSkipOfflineON(CSName, ClientName, InstanceName,subclientName):

	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML - ENABLE SKIP OFFLINE TABLESPACES")

		cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -skipOffline true -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setSubclientPropSkipOfflineON")
			return (True, _retval)
		else:
			log.error("Failed to setSubclientPropSkipOfflineON")
			return (False, _retval)
	except:
		log.exception("Exception raised at setSubclientPropSkipOfflineON")
		return (-1, False)

def setSubclientPropSkipOfflineOFF(CSName, ClientName, InstanceName,subclientName):

	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML - DISABLE SKIP OFFLINE TABLESPACES")

		cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -skipOffline false -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setSubclientPropSkipOfflineOFF")
			return (True, _retval)
		else:
			log.error("Failed to setSubclientPropSkipOfflineOFF")
			return (False, _retval)
	except:
		log.exception("Exception raised at setSubclientPropSkipOfflineOFF")
		return (-1, False)

def setSubclientPropSkipReadOnlyON(CSName, ClientName, InstanceName,subclientName):

	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML - ENABLE SKIP READONLY TABLESPACES")

		cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -skipReadOnly true -clientName " + ClientName + " -cs " +CSName+  " -instanceName " + InstanceName + " -subclientName " + subclientName

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setSubclientPropSkipReadOnlyON")
			return (True, _retval)
		else:
			log.error("Failed to setSubclientPropSkipReadOnlyON")
			return (False, _retval)
	except:
		log.exception("Exception raised at setSubclientPropSkipReadOnlyON")
		return (-1, False)

def setSubclientPropSkipReadOnlyOFF(CSName, ClientName, InstanceName,subclientName):

	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML - DISABLE SKIP READONLY TABLESPACES")

		cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -skipReadOnly false -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setSubclientPropSkipReadOnlyOFF")
			return (True, _retval)
		else:
			log.error("Failed to setSubclientPropSkipReadOnlyOFF")
			return (False, _retval)
	except:
		log.exception("Exception raised at setSubclientPropSkipReadOnlyOFF")
		return (-1, False)

def setSubclientPropSelectiveOnlineFullON(CSName, ClientName, InstanceName, subclientName):

	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML - ENABLE SELECTIVE ONLINE FULL")

		cmd = "qoperation execute -af " + xmlfile + " -appName Oracle -selectiveOnlineFull true -clientName " + ClientName + " -cs " +CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setSubclientPropSelectiveOnlineFullON")
			return (True, _retval)
		else:
			log.error("Failed to setSubclientPropBackUpControlFileONOrOFF")
			return (False, _retval)
	except:
		log.exception("Exception raised at setSubclientPropSelectiveOnlineFullON")
		return (-1, False)

def EnableSubclientSnapProtectDetails(CSName, ClientName, InstanceName, subclientName,SnapVendorName):

	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML - ENABLE SNAP PROTECT DETAILS")

		cmd = "qoperation execute -af " + xmlfile + " -appName Oracle -isRMANEnableForTapeMovement true -isSnapBackupEnabled true -clientName " + ClientName + " -cs " +CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName + " -snapShotEngineName " "\'" + SnapVendorName + "\'"

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully EnableSubclientSnapProtectDetails")
			return (True, _retval)
		else:
			log.error("Failed to EnableSubclientSnapProtectDetails")
			return (False, _retval)
	except:
		log.exception("Exception raised at EnableSubclientSnapProtectDetails")
		return (-1, False)

def setSubclientPropSelectiveOnlineFullOFF(CSName, ClientName, InstanceName, subclientName):

	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML - DISABLE SELECTIVE ONLINE FULL")

		cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -selectiveOnlineFull false -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setSubclientPropSelectiveOnlineFullOFF")
			return (True, _retval)
		else:
			log.error("Failed to setSubclientPropSelectiveOnlineFullOFF")
			return (False, _retval)
	except:
		log.exception("Exception raised at setSubclientPropSelectiveOnlineFullOFF")
		return (-1, False)

def setSubclientPropDataBackUpStreams(CSName, ClientName, InstanceName, subclientName, streamValue):
        return(subclienthelper.setSubClientProp(subclientName,"Oracle Data backup streams","7",streamValue,CSName,ClientName, \
                                            AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
                                            InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))

def setSubclientPropArchiveLogDeleteOFF(CSName, ClientName, InstanceName, subclientName):


	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML")

		cmd = "qoperation execute -af "+xmlfile+ " -appName Oracle -archiveDelete false -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setSubclientPropArchiveLogDeleteOFF")
			return (True, _retval)
		else:
			log.error("Failed to setSubclientPropArchiveLogDeleteOFF")
			return (False, _retval)
	except:
		log.exception("Exception raised at setSubclientPropArchiveLogDeleteOFF")
		return (-1, False)

def setSubclientPropArchiveLogDeleteON(CSName, ClientName, InstanceName, subclientName):

    log = loghelper.getLog()

    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

    try:
            log.info("Update Subclient Properties using XML")

            cmd = "qoperation execute -af "+xmlfile+ " -appName Oracle -archiveDelete true -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setSubclientPropArchiveLogDeleteON")
                return (True, _retval)
            else:
                log.error("Failed to setSubclientPropArchiveLogDeleteON")
                return (False, _retval)
    except:
            log.exception("Exception raised at setSubclientPropArchiveLogDeleteON")
            return (-1, False)


def setSubclientPropTableBrowseON(CSName, ClientName, InstanceName, subclientName):
    
        log = loghelper.getLog()
        scriptspath = cmdhelper.getScriptsPath()

        xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

        try:
            log.info("Update Subclient Properties using XML")

            cmd = "qoperation execute -af "+xmlfile+ " -appName Oracle -enableTableBrowse true -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setSubclientPropTableBrowseON")
                return (True, _retval)
            else:
                log.error("Failed to setSubclientPropTableBrowseON")
                return (False, _retval)
        except:
            log.exception("Exception raised at setSubclientPropTableBrowseON")
            return (-1, False)
        
def setSubclientPropTableBrowseOFF(CSName, ClientName, InstanceName, subclientName):
    
        log = loghelper.getLog()

        scriptspath = cmdhelper.getScriptsPath()
        xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

        try:
            log.info("Update Subclient Properties using XML")

            cmd = "qoperation execute -af "+xmlfile+ " -appName Oracle -enableTableBrowse true -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setSubclientPropTableBrowseOFF")
                return (True, _retval)
            else:
                log.error("Failed to setSubclientPropTableBrowseOFF")
                return (False, _retval)
        except:
            log.exception("Exception raised at setSubclientPropTableBrowseOFF")
            return (-1, False)
        
def setSubclientPropOfflineDatabase(CSName, ClientName, InstanceName, subclientName):

    """
    If we want to enable Offline Database then we need to disable the BackUp recovery and Archive Log
    """
    log = loghelper.getLog()
    try:
        log.info("Disable Protect BackUp Recovery OFF")
        retCodeR, retStringR = setSubclientProProtectBackupAndRecoveryOFF(CSName, ClientName, InstanceName, subclientName)

        log.info("Back Up Archive Log OFF")
        retCodeA, retStringA = setSubclientPropBackUpArchiveLogOFF(CSName, ClientName, InstanceName, subclientName)
        log.info("setting Oracle Mode to Offline Data Base")
        retCodeO, retStringO = subclienthelper.setSubClientProp(subclientName,"Oracle Backup Mode","7","2",CSName,ClientName, \
                                                AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
                                                InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME)
        if (retCodeR, retStringR) and (retCodeA, retStringA) and (retCodeO, retStringO) == (0, True):
            return (0, True)
        else:
            (1, False)
    except:
        log.exception("Exception raised at setSubclientPropOfflineDatabase")
        return (-1, False)

def setAutoBackupON(CSName, ClientName, InstanceName):

    log = loghelper.getLog()
    scriptspath = cmdhelper.getScriptsPath()

    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\UpdateInstance_Template.xml")

    try:
            log.info("Update Subclient Properties using XML")

            cmd = "qoperation execute -af "+xmlfile+ " -appName Oracle -ctrlFileAutoBackup '1' -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setAutoBackupON")
                return (True, _retval)
            else:
                log.error("Failed to setAutoBackupON")
                return (False, _retval)
    except:
            log.exception("Exception raised at setAutoBackupON")
            return (-1, False)
        
def getSBTLirary(CSName, ClientName, InstanceName):
    return(instancehelper.getInstanceProperty(CSName,ClientName, AutomationConstants.ORACLE_DataAgentType, InstanceName, \
                                              "Oracle SBT Library Name"))


def setSubclientPropOnlineDatabase(CSName, ClientName, InstanceName, subclientName):

    log = loghelper.getLog()
    scriptspath = cmdhelper.getScriptsPath()

    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

    try:
            log.info("Update Subclient Properties using XML")

            cmd = "qoperation execute -af "+xmlfile+ " -appName Oracle -data true -backupMode ONLINE_DB -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setSubclientPropOnlineDatabase")
                return (True, _retval)
            else:
                log.error("Failed to setSubclientPropOnlineDatabase")
                return (False, _retval)
    except:
            log.exception("Exception raised at setSubclientPropOnlineDatabase")
            return (-1, False)

def setSubclientPropBackUpArchiveLogON(CSName, ClientName, InstanceName, subclientName):

	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML")
		cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -backupArchiveLog true -clientName " + ClientName + " -cs " + CSName +" -instanceName " + InstanceName + " -subclientName " + subclientName

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setSubclientPropArchiveLogDeleteOFF")
			return (True, _retval)
		else:
			log.error("Failed to setSubclientPropArchiveLogDeleteOFF")
			return (False, _retval)
	except:
		log.exception("Exception raised at setSubclientPropArchiveLogDeleteOFF")
		return (-1, False)
		
		

def setSubclientPropBackUpArchiveLogOFF(CSName, ClientName, InstanceName, subclientName):

    """
        If we want to Disable Archive Log using XML
    """
    log = loghelper.getLog()

    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

    try:
            log.info("Update Subclient Properties using XML")

            cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -backupArchiveLog false -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setSubclientPropBackUpArchiveLogOFF")
                return (True, _retval)
            else:
                log.error("Failed to setSubclientPropBackUpArchiveLogOFF")
                return (False, _retval)
    except:
            log.exception("Exception raised at setSubclientPropBackUpArchiveLogON")
            return (-1, False)

def setSubclientProProtectBackupAndRecoveryOFF(CSName, ClientName, InstanceName, subclientName):
	return(subclienthelper.setSubClientProp(subclientName,"Oracle Protect Backup Recovery Area","2","0",CSName,ClientName, \
			AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
			InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))
def setSubclientProProtectBackupAndRecoveryON(CSName, ClientName, InstanceName, subclientName):
    return(subclienthelper.setSubClientProp(subclientName,"Oracle Protect Backup Recovery Area","2","1",CSName,ClientName, \
                                            AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
                                            InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))
def setAutoBackupONOrOFF(CSName, ClientName, InstanceName, OnOrOFF):
    return(instancehelper.setInstanceProperty(CSName,ClientName, AutomationConstants.ORACLE_DataAgentType, InstanceName, \
                                              "Auto Backup Control File" , 10, OnOrOFF))

def setSubclientPropDataOFF(CSName, ClientName, InstanceName, subclientName):
    return(subclienthelper.setSubClientProp(subclientName,"Oracle Backup Mode","7","1",CSName,ClientName, \
                                            AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
                                            InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))
def setSubclientPropBackUpControlFileONOrOFF(CSName, ClientName, InstanceName, subclientName, Enable):

        log = loghelper.getLog()
        scriptspath = cmdhelper.getScriptsPath()
        xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")


        try:
            log.info("Update Subclient Properties using XML - ENABLE/DISABLE CONTROLFILE BACKUP")
            if Enable == 0:
                cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -backupControlFile false -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName
            else:
                cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -backupControlFile true -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setSubclientPropBackUpControlFileONOrOFF")
                return (True, _retval)
            else:
                log.error("Failed to setSubclientPropBackUpControlFileONOrOFF")
                return (False, _retval)
        except:
            log.exception("Exception raised at setSubclientPropBackUpControlFileONOrOFF")
            return (-1, False)

def setSubclientPropSwitchCurrentLogONOrOFF(CSName, ClientName, InstanceName, subclientName, OnOrOFF):
    return(subclienthelper.setSubClientProp(subclientName,"Oracle LOS disable Switch","2",OnOrOFF,CSName,ClientName, \
                                            AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
                                            InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))

def setSubclientPropBackUpArchiveLogDestination(CSName, ClientName, InstanceName, subclientName, BackupArchiveLogDir):
        log = loghelper.getLog()
        scriptspath = cmdhelper.getScriptsPath()
        xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

        try:
            log.info("Update Subclient Properties using XML - setSubclientPropBackUpArchiveLogDestination")
            cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -backupArchiveLog true -selectArchiveLogDestForBackup true -archiveLogDestForBackupOpType ADD -archiveLogDestForBackup " + BackupArchiveLogDir + " -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName
            
            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setSubclientPropBackUpArchiveLogDestination")
                return (True, _retval)
            else:
                log.error("Failed to setSubclientPropBackUpArchiveLogDestination")
                return (False, _retval)
        except:
            log.exception("Exception raised at setSubclientPropBackUpArchiveLogDestination")
            return (-1, False)

def setSubclientPropDeleteArchiveLogDestination(CSName, ClientName, InstanceName, subclientName, DeleteArchiveLogDir):
    return(subclienthelper.setSubClientProp(subclientName,"Oracle Delete Archive Log Destinations","1",DeleteArchiveLogDir,CSName,ClientName, \
                                            AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
                                            InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))

def setSubclientPropBackUpArchiveLogFlagONOrOFF(CSName, ClientName, InstanceName, subclientName, Enable):

        log = loghelper.getLog()
        scriptspath = cmdhelper.getScriptsPath()
        xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")
        
        try:
            log.info("Update Subclient Properties using XML - ENABLE/DISABLE BACKUP ARCHIVELOG")
            if Enable == 0:
                cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -selectArchiveLogDestForBackup false -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName
            else:
                cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -selectArchiveLogDestForBackup true -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setSubclientPropBackUpArchiveLogFlagONOrOFF")
                return (True, _retval)
            else:
                log.error("Failed to setSubclientPropBackUpArchiveLogFlagONOrOFF")
                return (False, _retval)
        except:
                log.exception("Exception raised at setSubclientPropBackUpArchiveLogFlagONOrOFF")
                return (-1, False)

def setSubclientPropDeleteArchiveLogFlagONOrOFF(CSName, ClientName, InstanceName, subclientName, Enable):

        log = loghelper.getLog()
        scriptspath = cmdhelper.getScriptsPath()
        xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")
        
        try:
            log.info("Update Subclient Properties using XML - ENABLE/DISABLE DELETE ARCHIVELOG")
            if Enable == 0:
                cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -selectArchiveLogDestForDelete false -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName
            else:
                cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -selectArchiveLogDestForDelete true -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setSubclientPropDeleteArchiveLogFlagONOrOFF")
                return (True, _retval)
            else:
                log.error("Failed to setSubclientPropDeleteArchiveLogFlagONOrOFF")
                return (False, _retval)
        except:
            log.exception("Exception raised at setSubclientPropDeleteArchiveLogFlagONOrOFF")
            return (-1, False)

def setCatalogONOrOFF(CSName, ClientName, InstanceName, OnOrOFF):
    return(instancehelper.setInstanceProperty(CSName,ClientName, AutomationConstants.ORACLE_DataAgentType, InstanceName, \
                                              "use Catalog Connect" , 2, OnOrOFF))

def setSubclientPropOracleTag(CSName, ClientName, InstanceName, subclientName, OracleTag):
    return(subclienthelper.setSubClientProp(subclientName,"Oracle Tag","1",str(OracleTag),CSName,ClientName, \
                                            AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
                                            InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))

def setSubclientPropEncryption(CSName, ClientName, InstanceName, subclientName, Enable):
    return(subclienthelper.setSubClientProp(subclientName,"Encrypt: encryption","10",str(Enable),CSName,ClientName, \
                                            AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
                                            InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))
def setSubclientPropSkipInaccesible(CSName, ClientName, InstanceName, subclientName, Enable):
    log = loghelper.getLog()

    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

    try:
            log.info("Update Subclient Properties using XML - ENABLE/DISABLE SKIP OFFLINE")
            if Enable == 0:
                cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -skipInaccessible false -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName
            else:
                cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -skipInaccessible true -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setSubclientPropSkipOffline")
                return (True, _retval)
            else:
                log.error("Failed to setSubclientPropSkipOffline")
                return (False, _retval)
    except:
            log.exception("Exception raised at setSubclientPropSkipOffline")
            return (-1, False)

def setSubclientPropSkipOffline(CSName, ClientName, InstanceName, subclientName, Enable):

    log = loghelper.getLog()

    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

    try:
            log.info("Update Subclient Properties using XML - ENABLE/DISABLE SKIP OFFLINE")
            if Enable == 0:
                cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -skipOffline false -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName
            else:
                cmd = "qoperation execute -af " +xmlfile+ " -appName Oracle -skipOffline true -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setSubclientPropSkipOffline")
                return (True, _retval)
            else:
                log.error("Failed to setSubclientPropSkipOffline")
                return (False, _retval)
    except:
            log.exception("Exception raised at setSubclientPropSkipOffline")
            return (-1, False)

def setSubclientPropResyncCatalog(CSName, ClientName, InstanceName, subclientName, Enable):
    """
    To enable the subclient property we need to enter 1,to disable the property enter 0
    """

    return(subclienthelper.setSubClientProp(subclientName,"Oracle Resync Catalog","2",str(Enable),CSName,ClientName, \
                                            AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
                                            InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))
										
	###"""
	##Code added for Oracle RAC Subclient -Navatha Chintala --02/09/2017 ---Begin
	##"""
		
def setRACSubclientPropBackUpArchiveLogON(CSName, ClientName, InstanceName, subclientName):

	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML")
		cmd = "qoperation execute -af " +xmlfile+ " -appName 'Oracle RAC' -backupArchiveLog true -clientName " + ClientName + " -cs " + CSName +" -instanceName " + InstanceName + " -subclientName " + subclientName

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setRACSubclientPropBackUpArchiveLogON")
			return (True, _retval)
		else:
			log.error("Failed to setRACSubclientPropBackUpArchiveLogON")
			return (False, _retval)
	except:
		log.exception("Exception raised at setRACSubclientPropBackUpArchiveLogON")
		return (-1, False)
		
		
		
def setRACSubclientPropArchiveLogDeleteOFF(CSName, ClientName, InstanceName, subclientName):


	log = loghelper.getLog()

	scriptspath = cmdhelper.getScriptsPath()
	xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

	try:
		log.info("Update Subclient Properties using XML")

		cmd = "qoperation execute -af "+xmlfile+ " -appName 'Oracle RAC' -archiveDelete false -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

		(_retcode, _output) = cmdhelper.executeCommand(cmd)
		_retval = str(_output)
		log.info("OUTPUT IS" + _retval)

		if _retval.find("<errorCode>0</errorCode>") >= 0:
			log.info("Successfully setRACSubclientPropArchiveLogDeleteOFF")
			return (True, _retval)
		else:
			log.error("Failed to setRACSubclientPropArchiveLogDeleteOFF")
			return (False, _retval)
	except:
		log.exception("Exception raised at setRACSubclientPropArchiveLogDeleteOFF")
		return (-1, False)		
		

def setRACSubclientPropBackUpArchiveLogOFF(CSName, ClientName, InstanceName, subclientName):

    """
        If we want to Disable Archive Log using XML
    """
    log = loghelper.getLog()

    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

    try:
            log.info("Update Subclient Properties using XML")

            cmd = "qoperation execute -af " +xmlfile+ " -appName 'Oracle RAC' -backupArchiveLog false -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setRACSubclientPropBackUpArchiveLogOFF")
                return (True, _retval)
            else:
                log.error("Failed to setRACSubclientPropBackUpArchiveLogOFF")
                return (False, _retval)
    except:
            log.exception("Exception raised at setRACSubclientPropBackUpArchiveLogON")
            return (-1, False)		
			
def setRACSubclientPropOnlineDatabase(CSName, ClientName, InstanceName, subclientName):

    log = loghelper.getLog()
    scriptspath = cmdhelper.getScriptsPath()

    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

    try:
            log.info("Update Subclient Properties using XML")

            cmd = "qoperation execute -af "+xmlfile+ " -appName 'Oracle RAC' -backupMode ONLINE_DB -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setRACSubclientPropOnlineDatabase")
                return (True, _retval)
            else:
                log.error("Failed to setRACSubclientPropOnlineDatabase")
                return (False, _retval)
    except:
            log.exception("Exception raised at setRACSubclientPropOnlineDatabase")
            return (-1, False)		
def setRACSubclientPropSkipInaccesible(CSName, ClientName, InstanceName, subclientName, Enable):
    return(subclienthelper.setSubClientProp(subclientName,"Oracle Skip inaccessible","7",str(Enable),CSName,ClientName, \
                                            'Oracle RAC', AutomationConstants.App_Group_DataBase, \
                                            InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))

def setRACSubclientPropSkipOffline(CSName, ClientName, InstanceName, subclientName, Enable):

    log = loghelper.getLog()

    scriptspath = cmdhelper.getScriptsPath()
    xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

    try:
            log.info("Update Subclient Properties using XML - ENABLE/DISABLE SKIP OFFLINE")
            if Enable == 0:
                cmd = "qoperation execute -af " +xmlfile+ " -appName 'Oracle RAC' -skipOffline false -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName
            else:
                cmd = "qoperation execute -af " +xmlfile+ " -appName 'Oracle RAC' -skipOffline true -clientName " + ClientName + " -cs " +CSName+ " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setRACSubclientPropSkipOffline")
                return (True, _retval)
            else:
                log.error("Failed to setRACSubclientPropSkipOffline")
                return (False, _retval)
    except:
            log.exception("Exception raised at setRACSubclientPropSkipOffline")
            return (-1, False)
			
		
		
	###"""
	###Code added for Oracle RAC Subclient -Navatha Chintala --02/09/2017 --- End
	###"""	

	###"""
	###Code added for Oracle RAC Subclient -Navatha Chintala --03/24/2017 --- Begin
	###"""	
def setRACSubclientPropTableBrowseON(CSName, ClientName, InstanceName, subclientName):
    
        log = loghelper.getLog()
        scriptspath = cmdhelper.getScriptsPath()

        xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

        try:
            log.info("Update Subclient Properties using XML")

            cmd = "qoperation execute -af "+xmlfile+ " -appName 'Oracle RAC' -enableTableBrowse true -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setSubclientPropTableBrowseON")
                return (True, _retval)
            else:
                log.error("Failed to setRACSubclientPropTableBrowseON")
                return (False, _retval)
        except:
            log.exception("Exception raised at setRACSubclientPropTableBrowseON")
            return (-1, False)
        
def setRACSubclientPropTableBrowseOFF(CSName, ClientName, InstanceName, subclientName):
    
        log = loghelper.getLog()

        scriptspath = cmdhelper.getScriptsPath()
        xmlfile = os.path.join(scriptspath,"oraclehelper\\XML\\update_subclient_template.xml")

        try:
            log.info("Update Subclient Properties using XML")

            cmd = "qoperation execute -af "+xmlfile+ " -appName 'Oracle RAC' -enableTableBrowse true -clientName " + ClientName + " -cs " + CSName + " -instanceName " + InstanceName + " -subclientName " + subclientName

            (_retcode, _output) = cmdhelper.executeCommand(cmd)
            _retval = str(_output)
            log.info("OUTPUT IS" + _retval)

            if _retval.find("<errorCode>0</errorCode>") >= 0:
                log.info("Successfully setRACSubclientPropTableBrowseOFF")
                return (True, _retval)
            else:
                log.error("Failed to setRACSubclientPropTableBrowseOFF")
                return (False, _retval)
        except:
            log.exception("Exception raised at setRACSubclientPropTableBrowseOFF")
            return (-1, False)		
			
	###"""
	###Code added for Oracle RAC Subclient -Navatha Chintala --03/24/2017 --- End
	###"""				
	

def getControlFile(curs):
    """
    This function returns control file path
    """
    log = loghelper.getLog()
    try:
        #curs.execute("select status from v$instance")
        curs.execute("select name from v$controlfile")
        row = curs.fetchone()
        firstrow=row[0]
        return(0,firstrow)

    except:
        log.exception("Exception raised at getDatafile")
        return (-1, -1)

def listDatafiles(curs):
    """
    This Function gets all the datafiles in the database and returns their path
    """
    log = loghelper.getLog()
    try:
        curs.execute("select name from v$datafile")
        Datafile = curs.fetchall()
        #log.info("number of rows is " +str(rows))
        #Datafiles=rows
        log.info("Datafiles we got in the database are" + str(Datafile))
        return (0,Datafile, curs.rowcount)
    except:
        log.exception("Exception raised to get datafiles")
        return (0,"",0)

def getlistofredologs(curs):
    """
    This functions finds the all the redologs
    """
    log = loghelper.getLog()
    try:
        curs.execute("select member from v$logfile")
        Redologfile = curs.fetchall()
        #log.info("number of rows is " +str(rows))
        #Datafiles=rows
        log.info("Redologfiles we got in the database are" + str(Redologfile))
        return (0,Redologfile, curs.rowcount)
    except:
        log.exception("Exception raised to get Redologfiles")
        return (0,"",0)

def createStartupSqlFile():
    """
    shutdown and put database in nomount mode.Used for Test case 5071
    """
    log = loghelper.getLog()
    try:
        (_retCode, CommonDirPath) = getCommonDirPath()
        fp=open(CommonDirPath+'Startup.sql','w')
        fp.write('startup;\n')
        fp.write('exit;\n')
        fp.close()
        log.info("Created Startupsql file successfully")
        return(0, True)
    except:
        log.exception("INside createStartupqlFile")
        return(-1, False)


def restoreStandbyDBValidate(StandbyDBConnectUser, StandDBConnectPassword, StandbyDBConnectString, \
                               LocalPFilePath, ConnectUser, ConnectPassword, ConnectString):
    """
    Compares the datafile locations in the given PFile and from the Standby databse after restore. Incase of same path structure on both source and destiantion it will check
    """
    log = loghelper.getLog()
    try:
        conn = cx_Oracle.connect(StandbyDBConnectUser,StandDBConnectPassword, StandbyDBConnectString, cx_Oracle.SYSDBA)
        curs = conn.cursor()

        curs.execute('select status from v$instance')
        row = curs.fetchone()
        if row[0] != "MOUNTED":
            log.error("Restore job did not complete successfully")
            return (-1, False)
        else:
            log.info("Restore has completed successfully")
            log.info("Checking for datafile/tempfile/redolog fiel locations")
            curs.execute('select name from v$datafile')
            datafile = curs.fetchall()
            curs.execute('select name from v$tempfile')
            tempfile = curs.fetchall()
            curs.execute('select member from v$logfile')
            logfile = curs.fetchall()
            if os.path.isfile(LocalPFilePath):
                fd=open(LocalPFilePath,'r')
                count = 0
                for ln in fd.readlines():
                    if count ==2:
                        break
                    if ln.find("DB_FILE_NAME_CONVERT") >= 0 or ln.find("LOG_FILE_NAME_CONVERT") >= 0 :
                        count = count+1
                        ln = ln.replace(" ","")
                        ln = ln.replace(")",",")
                        ln = ln.split(',')
                        i = 1
                        while i < len(ln) :

                            if (str(datafile).find(ln[i]) >= 0):
                                log.info("datafile macthes" +ln[i])
                            elif (str(tempfile).find(ln[i]) >= 0):
                                log.info("tempfile macthes" +ln[i])
                            elif (str(logfile).find(ln[i]) >=0) :
                                log.info("logfile %s macthes" +ln[i])
                            else:
                                log.info("Datafile/tempfile/logfile is not matched" +ln[i])
                                return (-1, False)
                            i = i+2

            else:
                log.info("PFIle is not exist. Please cehcke")
                return (-1, False)
            if count == 2:
                log.info("all Datafiles and logfiles are matched")
                return (count,True)
            else :
                log.info("either DB_FILE_NAME_CONVERT or LOG_FILE_NAME_CONVERT OR Both may not preseent in PFILE. ")
                conn1 = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
                curs1 = conn1.cursor()
                curs1.execute("select name from v$datafile")
                sourcedatafiles = curs1.fetchall()
                curs1.execute("select member from v$logfile")
                sourcelogfiles = curs1.fetchall()

                curs.execute("select name from v$datafile")
                destdatafiles = curs.fetchone()
                curs.execute("select member from v$logfile")
                destlogfiles = curs.fetchone()

                print sourcelogfiles
                print destlogfiles

                if str(sourcedatafiles).find(str(destdatafiles)) >= 0:
                    log.info("Source and Destination datafile locations are matched")
                    if str(sourcelogfiles).find(str(destlogfiles)) >= 0:
                        log.info("Source and Destination log files are matched.")
                    else:
                        log.info("may be LOG_FILE_NAME CONVERT present in PFile")
                elif str(sourcelogfiles).find(str(destlogfiles)) >= 0:
                    log.info("Source and Destination log files are matched. for datafiles DB_FILE_NAME_CONVERT is present in PFile")
                else:
                        log.info("source and destiantion locations are not matched")
                        return(count, False)
                return (count, True)


    except:
        log.exception("Exception while Running restoreStandbyDBValidate function")
        return (-1, False)

def STandbylogshippingValidation(StandbyDBConnectUser, StandDBConnectPassword, StandbyDBConnectString,\
                               LocalPFilePath, ConnectUser, ConnectPassword, ConnectString):
    """
    This function compares the sequence number on both source database and standby database.if sequnce number is same logs are applied to standby DB
    """
    log = loghelper.getLog()
    try:

        conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
        curs = conn.cursor()
        (retCode, srcSeq)=GetArchiveLSN(curs)
        if retCode != 0:
            log.error("Failed to get Archive SCN")
            raise
        else:
            log.info ("Arch Log sequence number at source " +  str(srcSeq))

        conn1 = cx_Oracle.connect(StandbyDBConnectUser, StandDBConnectPassword, StandbyDBConnectString, cx_Oracle.SYSDBA)
        curs1 = conn1.cursor()
        cmd="select sequence# from (select sequence# from v$archived_log where archived='YES' order by first_time desc) where rownum=1"
        log.info(cmd)
        curs1.execute(cmd)
        sequence1 = curs1.fetchone()
        log.info("Sequence number archived is from stand by database is " + str(sequence1[0]))

        if str(srcSeq) != str(sequence1[0]):
            log.error ("Archive Log sequence number at source " + str(srcSeq) + " is not equal to standby " + str(sequence1[0]))
            return (1,False)
        else:
            log.error ("Archive Log sequence number at source " + str(srcSeq) + " is equal to standby " + str(sequence1[0]))
            return (0,True)
    except:
        log.exception("Exception raised to get STandbylogshippingValidation")
        return (1, False)

def getControlFiles(curs):
    """
    This function returns control file path
    """
    log = loghelper.getLog()
    try:
        #curs.execute("select status from v$instance")
        curs.execute("select name from v$controlfile")
        controlfiles = curs.fetchall()
        log.info("Controlfiles we got are " + str(controlfiles))
        return(0,controlfiles, curs.rowcount)
    except:
        log.exception("Exception raised at getcontrolfile")
        return (-1, "",0)

def listDatafiles(curs):
    """
    This Function gets all the datafiles in the database and returns their path
    """
    log = loghelper.getLog()
    try:
        curs.execute("select name from v$datafile")
        Datafile = curs.fetchall()
        #log.info("number of rows is " +str(rows))
        #Datafiles=rows
        log.info("Datafiles we got in the database are" + str(Datafile))
        return (0,Datafile, curs.rowcount)
    except:
        log.exception("Exception raised to get datafiles")
        return (0,"",0)


def createStartupnomountSqlFile():
    """
    shutdown and put database in nomount mode.Used for Test case 5071
    """
    log = loghelper.getLog()
    try:
        (_retCode, CommonDirPath) = getCommonDirPath()
        fp=open(CommonDirPath+'dbnomount.sql','w')
        fp.write('shutdown immediate;\n')
        fp.write('startup nomount;\n')
        fp.write('exit;\n')
        fp.close()
        log.info("Created dbnomountsql file successfully")
        return(0, True)
    except:
        log.exception("INside createStartupnomountSqlFile")
        return(-1, False)

def getBackupEndTime3(CommServerName, jobId):
    """
    This function gets backup end time and round the second to 59 sec.This function is used for TC35489
    """
    log = loghelper.getLog()
    try:
        log.info ("get Backup End time" )

        (retCode, retVal) = dbhelper.readAndReplaceVariables("GetEndTime_BackupJob.sql",jobId )

        if retCode != 0:
            log.error("Error occured at while replacing values in sql file")
            return False

        TimeZone = dbhelper.getCVData(retVal, CommServerName)
        endTime = datetime.fromtimestamp(TimeZone[0][0])

        # round the seconds to 59. so that restore will always find till next minute.
        endTimelist = str(endTime).rsplit(":", 1)
        log.info("list is " +endTimelist[0])

        return endTimelist[0]+":59"

    except:
        log.exception("Exception raised at getBackupEndTime")
        return ""

def StandbyrestoreValidate(ConnectUser, ConnectPassword, ConnectString, DataFilePath=None, Mounted=False):
    #print("In restore Validation fuction of oraclehelper library ")
    """
    This function validates the database status and database role as standby
    """
    log = loghelper.getLog()
    try:
        conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
        curs = conn.cursor()
        if DataFilePath != None:
            curs.execute()

        curs.execute('select status from v$instance')
        row = curs.fetchone()
        if row[0] != "MOUNTED" :
            if row[0] == 'MOUNTED':
                log.info("Database Status is Mounted after restore Restore job, return ok")
                curs.execute('select database_role from v$database')
                row = curs.fetchone()
                if row[0] == 'STANDBY':
                    log.info("Role is standby")
                    return (0, True)
                else:
                    log.info("Role is notstandby " + str(row[0]))
                    return (1, False)
            else:
                log.error("Restore job did not complete successfully")
                return (1, False)
        else:
            log.info("Restore has completed successfully")
            return (0, True)
    except:
        log.exception("Exception while Running StandbyrestoreValidate function")
        return(-1, False)




def setSubclientPropUSESQLCONNECT(CSName, ClientName, InstanceName, subclientName, Enable):
    """
    To enable the subclient property for use sql connect in offline arcguments we need to enter 1,to disable the property enter 0
    """

    return(subclienthelper.setSubClientProp(subclientName,"Oracle Use SQL Connect","2",str(Enable),CSName,ClientName, \
                                            AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
                                            InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))

def setSubclientPropOracleDataBackupStreams(CSName, ClientName, InstanceName, subclientName, Enable):
    """
    To enable the subclient property for Oracle data backup streams.For enable enter the actaul number of streams like 2 or 3 or 4
    """

    return(subclienthelper.setSubClientProp(subclientName,"Oracle Data backup streams","7",str(Enable),CSName,ClientName, \
                                            AutomationConstants.ORACLE_DataAgentType, AutomationConstants.App_Group_DataBase, \
                                            InstanceName, AutomationConstants.ORACLE_DEF_BKPSET_NAME))

def getRemoteTempPath(sshConn, SimpanaBasePath):
    """
    returns directory location which will be used to store files getting created during runtime of TC
    """
    log = loghelper.getLog()
    try:
        if sshConn.opsystem.lower() == "windows":
            if SimpanaBasePath == None:
                SimpanaBasePath = "C:"
            elif SimpanaBasePath.find(" ") >= 0:
                SimpanaBasePath = SimpanaBasePath.split(":")
                SimpanaBasePath =  SimpanaBasePath[0] + ":"
            RemotePath =   SimpanaBasePath + r"\ORACLETemp\\"
            cmd = "cmd.exe /c mkdir " + RemotePath
        else:
            if SimpanaBasePath == None:
                SimpanaBasePath = "/tmp/"
            RemotePath = SimpanaBasePath + "/ORACLETemp/"
            cmd = "mkdir " + RemotePath
        output = sshConn.execute(cmd)
        log.info("Retrieved ORACLECommon directory path successfully on ORACLE Client machine")
        return RemotePath
    except:
        log.exception("Exception raised at getRemoteTempPath")
        return  False
def removeRemoteTempPath(sshConn,Path):
    """
    It removes remote DB2temp Path
    """
    log = loghelper.getLog()
    try:
        if sshConn.opsystem.lower() == "windows" :
            Path = Path.replace(r'\\', '\\')
            if Path.find(".txt") >= 0 or Path.find(".") >=0:
                cmd =  "cmd.exe /c del /s /Q "+ Path + " > " + "del.txt"
            else:
                cmd = "cmd.exe /c rmdir /Q " + Path
        else:

            cmd =  "rm -rf "+ Path

        retCode = sshConn.execute(cmd)
        log.info("Autoamtion temp files are deleted  " +str(retCode))
        print retCode
        return True

    except:
        log.exception("Exception raised in removeAutoamtionTempDirectory")
        return False

def runGalaxyReport(sshConn, CommServerName, ClientName, RemotePath, Type):
    """
    It run Galaxy reports and saved in pdf format
    """
    log = loghelper.getLog()
    try:
        CommServerTimeZone = getCommServerTimeZone(CommServerName)
        (retCode, commonDirPath) = getCommonDirPath()
        log.info("Local commonDirPath path we got is " + commonDirPath)

        retCode = removeRemoteTempPath(sshConn, RemotePath+Type+"Report.pdf")

        if Type == "Backup":

            cmd="qoperation execute -af BackupReport.xml -clientName " + ClientName + \
                                                  " -cs " + CommServerName + " -TimeZoneName \"" + str(CommServerTimeZone) +"\"" + \
                                                  " -locationURL '" + RemotePath + "BackupReport.pdf'"
            log.info(cmd)

            (_retCode,_data)=cmdhelper.executeCommand(cmd)
        elif Type == "Restore":
            (_retCode,_data)=cmdhelper.executeCommand(r"qoperation execute -af RestoreReport.xml -clientName " + ClientName + \
                                                  " -cs " + CommServerName + " -TimeZoneName \"" + str(CommServerTimeZone) +"\"" + \
                                                  " -locationURL '" + RemotePath + "RestoreReport.pdf'" )
        else:
            log.info("give either Backup or Restore for type")
            return -1
        if _retCode == -1:
            log.exception("Qoperation command failed to start " +Type + " Job Summary Report")
            return -1
        time.sleep(20)
        if sshConn.opsystem.lower() != 'windows':
            #cmd = "cd " + RemotePath + Type +"Report.pdf; cp * ../" + Type + "Report1; "
            cmd = "cp " + RemotePath + Type +"Report.pdf/* " + RemotePath  + Type + "Report1.pdf "
            log.info(cmd)
            (retCode,value)= sshConn.execute(cmd)
            log.info(retCode)
            log.info(value)
            sshConn.get(RemotePath + Type + 'Report1.pdf', commonDirPath + Type + 'Report.pdf')
        else:
            sshConn.get(RemotePath + Type + 'Report.pdf', commonDirPath + Type + 'Report')
        log.info("Restore Report is saved in "+ str(commonDirPath))
        return 0
    except:
        log.exception("Exception raised at runGalaxyReport")
        return -1

def RmanCrossCheckNocatalog(commonDirPath,ConnectUser, ConnectPassword, ConnectString):
    """
    This function is used to connect to rman and do cross check archivelog all.
    """
    log = loghelper.getLog()
    try:
        fd=open(commonDirPath+'crosscheck.sql', 'w')
        fd.write('crosscheck archivelog all;\n')
        fd.write('exit;\n')
        fd.close()
        log.info("created crosscheck srchivelog  file successfully")
    except:
        log.error("failed to rosscheck srchivelog  file with content as crosscheck archive log all command")
    ####We can't really connect to catalog Db from the controller machine.We need to update the tnsnames.ora file with catalog deatils.so commenting this code####
    #if CatalogConnectEnable.lower() == 'yes':
    #    return (cmdhelper.executeCommand("rman target " +ConnectUser+"/"+ConnectPassword+"@"
    #                                 +ConnectString+" " +CatalogUser+"/"+CatalogPassword+"@"+CatalogConnectString+"'"+commonDirPath + "crosscheck.sql\'" + " log=\'" +commonDirPath +"crosscheck.sql.log\'\""))
    #else:
    #     return (cmdhelper.executeCommand("rman target " +ConnectUser+"/"+ConnectPassword+"@"
    #                                 +ConnectString+" \"nocatalog @\'"+commonDirPath + "crosscheck.sql\'" + " log=\'" +commonDirPath +"crosscheck.sql.log\'\""))
    return (cmdhelper.executeCommand("rman target " +ConnectUser+"/"+ConnectPassword+"@"
                                     +ConnectString+" \"nocatalog @\'"+commonDirPath + "crosscheck.sql\'" + " log=\'" +commonDirPath +"crosscheck.sql.log\'\""))

def RmanCrossCheck(commonDirPath,ConnectUser, ConnectPassword, ConnectString,CatalogConnectEnable, CatalogUser,CatalogPassword, CatalogConnectString):
    """
    This function is used to connect to rman and do cross check archivelog all.
    """
    log = loghelper.getLog()
    try:
        fd=open(commonDirPath+'crosscheck.sql', 'w')
        fd.write('crosscheck archivelog all;\n')
        fd.write('exit;\n')
        fd.close()
        log.info("created crosscheck srchivelog  file successfully")
    except:
        log.error("failed to rosscheck srchivelog  file with content as crosscheck archive log all command")
    ####We can't really connect to catalog Db from the controller machine.We need to update the tnsnames.ora file with catalog deatils.so commenting this code####
    #if CatalogConnectEnable.lower() == 'yes':
    #    return (cmdhelper.executeCommand("rman target " +ConnectUser+"/"+ConnectPassword+"@"
    #                                 +ConnectString+" " +CatalogUser+"/"+CatalogPassword+"@"+CatalogConnectString+"'"+commonDirPath + "crosscheck.sql\'" + " log=\'" +commonDirPath +"crosscheck.sql.log\'\""))
    #else:
    #     return (cmdhelper.executeCommand("rman target " +ConnectUser+"/"+ConnectPassword+"@"
    #                                 +ConnectString+" \"nocatalog @\'"+commonDirPath + "crosscheck.sql\'" + " log=\'" +commonDirPath +"crosscheck.sql.log\'\""))
    return (cmdhelper.executeCommand("rman target " +ConnectUser+"/"+ConnectPassword+"@"
                                     +ConnectString+" \"nocatalog @\'"+commonDirPath + "crosscheck.sql\'" + " log=\'" +commonDirPath +"crosscheck.sql.log\'\""))


def RmanDiskBackup(ClientName, UserName, UserPassword, OSType, DBDumpPath, commonDirPath,ConnectUser, ConnectPassword, ConnectString):
    """
    This Function TAkes Rman Disk backup to specified default location such as DBDump path.If the BAckup can't be performed to the specifed location BAckup will be done to C:\TEmp\AUTODUMP
    """
    log = loghelper.getLog()
    try:
        Path = DBDumpPath
        print Path

        #Get sc scnnumber_Diskbackup.log from Oracle client. If we have an error
        #it means the file does not exist. so, run new backup.
        (retCode, LocalFilePath) = getFileFromUnixOrWindows(ClientName,
                                                            UserName,
                                                            UserPassword,
                                                            OSType, DBDumpPath, "scnnumber_diskbackup.log")
        print retCode
        print LocalFilePath
        backupExists= False
        log.info("check if disk backup exists")
        if retCode == 0:
            log.info("Rman disk backup exists in "+ DBDumpPath+ " So no need to take disk backup ")
            log.info("got backupscn file "+ LocalFilePath)
            backupExists=True
        else:
            print("###Code to start writing into RmanDiskBackup.sql file starts here###")
            fd=open(commonDirPath+'RmanDiskBAckup.sql', 'w')
            fd.write('shutdown immediate;\n')
            fd.write('startup mount;\n')
            fd.write('CONFIGURE CONTROLFILE AUTOBACKUP ON ;\n')
            fd=open(commonDirPath+'RmanDiskBAckup.sql', 'w')

            if OSType.lower() == "windows":
                log.info("This is windows test machine")
                fd.write("CONFIGURE CONTROLFILE AUTOBACKUP FORMAT FOR DEVICE TYPE DISK TO '" + DBDumpPath+"\%F' ;\n")
                fd.write('run{\n')
                fd.write("allocate channel ch1 type DISK ;\n")
                fd.write("allocate channel ch2 type DISK ;\n")
                fd.write("backup database format '" +DBDumpPath+"\\bkp_%U' tag =ORA_AUTO;\n")
            else:
                log.info("This is unix test machine")

                fd.write("CONFIGURE CONTROLFILE AUTOBACKUP FORMAT FOR DEVICE TYPE DISK TO '" + DBDumpPath+"/%F' ;\n")
                fd.write('run{\n')
                fd.write("allocate channel ch1 type DISK ;\n")
                fd.write("allocate channel ch2 type DISK ;\n")
                fd.write("backup database format '" +DBDumpPath+"/bkp_%U' tag =ORA_AUTO;\n")

            fd.write('release channel ch1;\n')
            fd.write('}\n')
            fd.write("CONFIGURE CONTROLFILE AUTOBACKUP FORMAT FOR DEVICE TYPE DISK TO '%F';\n")
            fd.write('Exit;\n')
            fd.close()
            log.info("created Created rmandiskbackup.sql file successfully")

            (retCode, retStr)=cmdhelper.executeCommand("rman target " +ConnectUser+"/"+ConnectPassword+"@"
                                    +ConnectString+" \"nocatalog @\'"+commonDirPath + "RmanDiskBAckup.sql\'" + " log=\'" +commonDirPath +"RmanDiskBAckup.sql.log\'\"")
            retCode=0
            if retCode == 0:

                log.info("Connect to Oracle")
                (retCode, retString, conn, curs) = connectOracle(ConnectUser, ConnectPassword, ConnectString)
                if retCode == -1:
                    log.error("OperationalError: ORA-01034: ORACLE not available")

                log.info("GetBackupSCN")
                #get backupscn
                (retCode,scn)=GetBackupSCNAfterDiskBackup(curs)
                if retCode != 0 :
                    log.info("Failed to get GetBackupSCNAfterDiskBackup")
                    raise

                log.info("GetDID")
                #get dbidId()
                (retCode,DBID)=Getdbid(curs)
                if retCode != 0:
                    log.info("Failed to get Getdbid")
                    raise

                #write to in dbdump path file
                (retCode, scnFilePath)=WriteSCNNumber(commonDirPath, DBID, scn)
                if retCode != 0:
                    log.info ("Failed to write scn number in local path")
                    raise


                if OSType.lower() == 'windows':
                    log.info("This is windows OS")


                    #put the file on remote machine
                    retString=remoteconnection.putOnRemoteMachine(ClientName,
                                                              scnFilePath,
                                                              DBDumpPath,
                                                              UserName,
                                                              UserPassword)
                    if retString != False:
                        log.info("copied " + scnFilePath +" to " +DBDumpPath )
                    else:
                        log.info("error in copying " + scnFilePath +" to " +DBDumpPath )
                        raise
                else:
                    remotefile=DBDumpPath+ r"/" +"scnnumber_diskbackup.log"
                    sshConn=remoteconnection.Connection(ClientName, OS="unix",
                                                username=UserName,
                                                password=UserPassword)
                    log.info ("copying scn number file to remote machine " + scnFilePath + "to " + remotefile)
                    sshConn.put(scnFilePath, remotefile)
                    log.info("copied " + scnFilePath +" to " +remotefile)


        return (0, "")

    except:
        log.error(" Falied to create rmandiskbackup.sql file command")
        return (-1, "")


def GetBackupSCNAfterDiskBackup(curs):
    """
    This function -actually- BAckup SCn number from the database
    """
    log = loghelper.getLog()
    try:
        cmd="alter system switch logfile"
        curs.execute(cmd)
        cmd1="select TO_CHAR(first_change#) from v$log where status = 'CURRENT'"
        log.info(cmd)
        try:
            curs.execute(cmd1)
        except:
            log.info("Some issue while getting cursor object")

        log.info(cmd)
        row = curs.fetchone()
        log.info ("BAckup SCN Number After Rman Disk backup is " + row[0])
        return (0, row[0])
    except:
        log.error("Failed to get GetBackupSCNNumber")
        return(-1, False)


def WriteSCNNumber(dirPath,DBID, SCNNumber):
    """
    This function writes SCNnumber into DBDumpPath and saves the SCN number
    """
    log = loghelper.getLog()
    try:
        scnFilePath=dirPath+"\scnnumber_diskbackup.log"
        fp=open(scnFilePath,'w')
        log.info("Sucessfully created SCN number log file on  the Windows DBDumpPath location.")
        fp.write(DBID)
        fp.write("\n")
        fp.write(SCNNumber)
        fp.close()
        log.info("SCN log file  is created sucessfully")
        return(0,scnFilePath)
    except:
        log.error("Failed to write SCN number into the DB dumpPath location")
        return(-1,"")

def RmanRestorefromDiskBackup(ClientName, UserName,UserPassword,OSType,DBDumpPath,commonDirPath,ConnectUser, ConnectPassword, ConnectString):
    """
    This Function TAkes Rman Disk restore from disk backup location such as DBDump path.If the BAckup can't be performed to the specifed location BAckup will be done to C:\TEmp\AUTODUMP
    """
    log = loghelper.getLog()
    try:

        (retCode, LocalFilePath) = getFileFromUnixOrWindows(ClientName,
                                                            UserName,
                                                            UserPassword,
                                                            OSType, DBDumpPath, "scnnumber_diskbackup.log")


        if retCode != 0:
            log.info ("error in getting backup scn log file from remote machine in path " + DBDumpPath)
            raise
        else:
            (retCode,dbid,scn)=ReadDBIDSCNNumber(LocalFilePath)

        #ALTER SYSTEM SET DB_RECOVERY_FILE_DEST = '' scope=both;

        log.info("dbid =" + dbid + " scn " +scn)
        log.info("Creating rman restore script")
        fd=open(commonDirPath+'RmanDiskRestore.sql', 'w')
        fd.write('shutdown abort;\n')
        fd.write('startup nomount;\n')
        fd.write("SET DBID "+ dbid  + " \n")
        #fd.write("CONFIGURE CONTROLFILE AUTOBACKUP FORMAT FOR DEVICE TYPE DISK TO '" + DBDumpPath+"\%F' ;\n")
                #fd.write("SET DBID "+ dbid  + " \n")
        fd.write('SET CONTROLFILE AUTOBACKUP FORMAT \n')
        if OSType.lower() == "windows":
            log.info("This is windows test machine")
            fd.write("FOR DEVICE TYPE DISK TO '"+ DBDumpPath +"\%F';\n")
        else:
            log.info("This is unix test machine")
            fd.write("FOR DEVICE TYPE DISK TO '"+DBDumpPath+"/%F';\n")

        fd.write('run{\n')
        fd.write("allocate channel ch1 type DISK ;\n")
        fd.write('RESTORE CONTROLFILE FROM AUTOBACKUP maxdays 350;\n')
        fd.write('alter database mount;\n')
        fd.write("CONFIGURE CONTROLFILE AUTOBACKUP FORMAT FOR DEVICE TYPE DISK TO '%F';\n")
        fd.write("restore database from tag 'ORA_AUTO';\n")
        fd.write("recover database  until scn " +  scn + ";\n")
        fd.write('alter database open resetlogs;\n')
        fd.write('}\n')
        fd.write('Exit;\n')
        fd.close()
        log.info("created Created rmandiskrestore.sql   file successfully")
    #except:
    #    log.error("Falied to created rmandisk restore.sql file command")
    #return(cmdhelper.executeCommand("rman target " +ConnectUser+"/"+ConnectPassword+"@"
    #                                 +ConnectString+" \"nocatalog @\'"+commonDirPath + "RmanDiskRestore.sql\'" + " log=\'" +commonDirPath +"RmanDiskRestore.sql.log\'\""))
        (retCode, retStr)= (cmdhelper.executeCommand("rman target " +ConnectUser+"/"+ConnectPassword+"@"
                                     +ConnectString+" \"nocatalog @\'"+commonDirPath + "RmanDiskRestore.sql\'" + " log=\'" +commonDirPath +"RmanDiskRestore.sql.log\'\""))
        if retCode == 1:
            log.info("Connect to Oracle")
            conn = cx_Oracle.connect(ConnectUser, ConnectPassword, ConnectString, cx_Oracle.SYSDBA)
            curs = conn.cursor()
            cmd="alter database open resetlogs"
            log.info(cmd)
            curs.execute("alter database open resetlogs")
        else:
            log.info("Database was recovered and opened with out any issue")
        return (0,"")
    except:
        log.error("Failed to create RmanDiskRestore.sql file command")
        return (-1, "")


def  ReadDBIDSCNNumber(scnfilepath):
    """
    This function reads the SCN number from RmanDisk Backup script and gets the current SCN number
    """
    log = loghelper.getLog()
    try:
        #sshConn=remoteconnection.Connection(pureClientName, OS="windows", username=RemoteMachineUserName, password=RemoteMachinePassword)
        fp=open(scnfilepath,'r')

        dbid=fp.readline()
        scn=fp.readline()

        fp.close()
        log.info("Sucessfully read the SCNNumber from the scnnumber_Diskbackup.log log file.")
        return(0,dbid, scn)
    except:
        log.error("Failed to read the SCNNumber from the scnnumber_Diskbackup.log log file")
        return(-1,0,0)


#get remote file from RemoteDirPath to commDir
def getFileFromUnixOrWindows(ClientName, UserName, Password, OSType, RemoteDirPath, RemoteFileName):

    """
    check for unix, windows(local, rmeote) platform and puul out pfile file accordingly
    """
    log = loghelper.getLog()
    (_retCode, commDir) = getCommonDirPath()
    LocalFile = os.path.join(commDir, RemoteFileName)


    try:
        if OSType.lower() == "windows":
            log.info("this is windows test machine")
            #SimpanaInstalledPath=cmdhelper.getBasePath()
            RemoteFile=RemoteDirPath+"\\"+ RemoteFileName
            if remoteconnection.getLocalHostName() != ClientName:
                retCode = remoteconnection.getFromRemoteMachine(ClientName, RemoteFile, LocalFile, UserName, Password)
                if str(retCode) != False:
                    log.info("successfully copied File from Windows client machine " + RemoteFile + " client " + ClientName + " to " + LocalFile)
                else:
                    log.error("Failed to copy File "+ RemoteFile + " from windows client machine")
                    return (1, "")
            else:

                log.info("seems we are running code from windows local machine which simpana client machine only")
                return (0, RemoteFile)

        else:
            log.info("this is linux test machine")
            try:
                RemoteFile=RemoteDirPath+"/"+ RemoteFileName
                sshConn=remoteconnection.Connection(ClientName, OS="unix", username=UserName, password=Password)
                sshConn.get(RemoteFile, LocalFile )
                log.info("successfully copied PFile from unix client machine")
            except:
                log.exception("Exception raised at GetPFileUnixMachine " + RemoteFile)
                return (1, "")

        #check if local file exists
        if os.path.exists(LocalFile) == True:
            log.info ("local file " + LocalFile + " exists")
            return (0, LocalFile)
        else:
            log.info ("local file " + LocalFile + "not exists")
            return (1, "")

    except:
        log.exception("Inside GetPFileFromUnixOrWindows")
        return (-1, "")


def setRmanWarningIgnoreKeyOnWindows(ClientHostName, RemoteMachineUserName, RemoteMachinePassword, SimpanaInstance, OSType):
    log = loghelper.getLog()
    try:
        #print("Hello")
        #log.info("Command sucess")
        Key_Path=("SOFTWARE\CommVault Systems\Galaxy" + "\\"+ SimpanaInstance + "\\OracleAgent")
        RemoteMachineUserName = RemoteMachineUserName.split('\\')
        UserName = RemoteMachineUserName[1]
        Domain = RemoteMachineUserName[0]

        try:
            keyHdl = Registryhelper.createOrOpenRegKey("sIGNORERMANWARNINGS", win32con.HKEY_LOCAL_MACHINE, ClientHostName, UserName, RemoteMachinePassword, windomain,None, False)
            (_value, _type) = _winreg.QueryValueEx(keyHdl, Key_Path)
            check = (_value == 'Y')
            return (0, 1)

        except:
            log.info ("regkey is not set. create new key")
            retcode = Registryhelper.createRegKeyEntry("sIGNORERMANWARNINGS", win32con.REG_SZ, "Yes", Key_Path, win32con.HKEY_LOCAL_MACHINE,\
                                                       ClientHostName, UserName, RemoteMachinePassword,Domain )
            if(retcode != 0):
                log.error("Error creating a registry key entry\n")
                return (-1, 0)
            else:
                log.info("sIGNORERMANWARNINGS key is created\n")

            return (0 , 2)

    except:

        log.exception("Some issue with setting regkey")
        return (-1, 0)
def setRmanWarningIgnoreKeyOnUnix(ClientHostName, RemoteMachineUserName, RemoteMachinePassword, SimpanaInstance, OSType):

    """
        This function creates the sIGNORERMANWARNINGS Y registry key on the remote Oracle Machine.
        returns False on error or UE.
    """
    log = loghelper.getLog()
    try:

        sshConn=remoteconnection.Connection(ClientHostName, OS="unix", username=RemoteMachineUserName, password=RemoteMachinePassword)
        cmd = ("sed '/sIGNORERMANWARNINGS/d'  /etc/CommVaultRegistry/Galaxy/Instance001/OracleAgent/.properties > /tmp/oraprop ;" + " echo 'sIGNORERMANWARNINGS Y' >> /tmp/oraprop;cat /tmp/oraprop > /etc/CommVaultRegistry/Galaxy/Instance001/OracleAgent/.properties")

        value = sshConn.execute(cmd)
        log.info("value is "+str(value))
        #log.info("retCode is " + str(retCode))
        sshConn.close()
        #return str(retCode[0][0]).replace('\n','')
        #return str(value[0]).replace('\n','')
        return str(value)
    except:
        log.exception("Exception raised at CreateRmanWarningIgnoreKeyOnUnix")
        return None
def DeleteRmanWarningIgnoreKeyOnUnix(ClientHostName, RemoteMachineUserName, RemoteMachinePassword, SimpanaInstance, OSType):

    """
        This function modifies the sIGNORERMANWARNINGS registry key and sets to N
        returns False on error or UE.
    """
    log = loghelper.getLog()
    try:

        sshConn=remoteconnection.Connection(ClientHostName, OS="unix", username=RemoteMachineUserName, password=RemoteMachinePassword)
        cmd = ("sed '/sIGNORERMANWARNINGS/d'  /etc/CommVaultRegistry/Galaxy/Instance001/OracleAgent/.properties > /tmp/oraprop ;" + " echo 'sIGNORERMANWARNINGS N' >> /tmp/oraprop;cat /tmp/oraprop > /etc/CommVaultRegistry/Galaxy/Instance001/OracleAgent/.properties")

        value = sshConn.execute(cmd)
        log.info("value is "+str(value))
        #log.info("retCode is " + str(retCode))
        sshConn.close()
        #return str(retCode[0][0]).replace('\n','')
        #return str(value[0]).replace('\n','')
        return str(value)
    except:
        log.exception("Exception raised at CreateRmanWarningIgnoreKeyOnUnix")
        return None

def CreateRmanWarningIgnore(ClientHostName, RemoteMachineUserName, RemoteMachinePassword, SimpanaInstance,OSType):
    """
    Accordong to OSTYPE i will call correxponding funtion and resturns home path
    """
    log = loghelper.getLog()
    try:
        if OSType.lower() == 'windows':
            path = setRmanWarningIgnoreKeyOnWindows(ClientHostName, RemoteMachineUserName, RemoteMachinePassword,SimpanaInstance,OSType)
        else:
            path = setRmanWarningIgnoreKeyOnUnix(ClientHostName, RemoteMachineUserName, RemoteMachinePassword,SimpanaInstance,OSType)
        return (path,2)
    except:
        log.exception("Exception raised in CreateRmanWarningIgnore")
        return (None,0)

def DeleteRmanWarningIgnore(ClientHostName, RemoteMachineUserName, RemoteMachinePassword, SimpanaInstance,OSType):
    """
    Accordong to OSTYPE i will call correxponding funtion and resturns home path
    """
    log = loghelper.getLog()
    try:
        if OSType.lower() == 'windows':
            path = deleteRmanWarningIgnoreKeyOnWindows(ClientHostName, RemoteMachineUserName, RemoteMachinePassword,SimpanaInstance,OSType)
        else:
            path = DeleteRmanWarningIgnoreKeyOnUnix(ClientHostName, RemoteMachineUserName, RemoteMachinePassword,SimpanaInstance,OSType)
        return path
    except:
        log.exception("Exception raised in DeleteRmanWarningIgnore")
        return None

def deleteRmanWarningIgnoreKeyOnWindows(ClientHostName, UserName, RemoteMachinePassword, SimpanaInstance, OSType):
    log = loghelper.getLog()
    try:
        print("Hello")
        log.info("Command sucess")

        #Key_Path="SOFTWARE\CommVault Systems\Galaxy" + "\\"+ SimpanaInstance + "\\OracleAgent" +"\\sIGNORERMANWARNINGS"
        Key_Path=("SOFTWARE\CommVault Systems\Galaxy" + "\\"+ SimpanaInstance + "\\OracleAgent\sIGNORERMANWARNINGS")

        try:

            retcode = Registryhelper.deleteRegKey( Key_Path, win32con.HKEY_LOCAL_MACHINE,\
                                                  ClientHostName, UserName, RemoteMachinePassword,Domain)
            #retcode = Registryhelper.createRegKeyEntry("sIGNORERMANWARNINGS", win32con.REG_SZ, "No", Key_Path, win32con.HKEY_LOCAL_MACHINE,\
            #                                           ClientHostName, "Administrator", "commvault!12", "inthost2-vm2")
            if(retcode != 0):
                log.error("Error deleting a registry key entry\n")
                return (-1, 0)
            else:
                log.info("sIGNORERMANWARNINGS key is removed\n")

            return (0 , 2)
        except:
            log.error("Some issue while deleting Registry ")
            return (-1,0)

    except:
        log.exception("Some issue with setting regkey")
        return (-1, 0)

def AuxCopy(Commserver,StoragePolicyName):

    log = loghelper.getLog()
    log.info("Starting an AuxCopy Job")
    jobId =""
    (_retcode, _data) = cmdhelper.executeCommand(r"qoperation execute -af auxcopy.xml -sp "+StoragePolicyName+" -cs "+Commserver)
    if _retcode != 0:
        log.exception("Error - Executing an AuxCopy Job")
        return _retcode
    else:
        jobId = cmdhelper.getJobId(_data)
        (_retcode, jobStatus) = cmdhelper.waitForJobToComplete(jobId, Commserver)
        if _retcode != 0:
            log.exception("JobID " + str(jobId) + " : Error - Job unable to complete")
            return _retcode

        if jobStatus == False:
            log.exception("JobID " + str(jobId) + " : Error - Job unable to complete and has False status")
            return 1
        else:
            log.info("JobID " + str(jobId) + " : Success - Executing an AuxCopy Job.")

    return (0,jobId)
def isRestoreDoneFromSeconadaryCopy(FileName, jobId,copyprecedence):
    """
    FileName: Full Path of ORASBT.log file.
    jobId: jobId for which we want to check Restore job is done from the secondary copy
    """
    log = loghelper.getLog()
    try:

        if os.path.isfile(FileName):
            fd = open(FileName,"rU")
            for line in fd:
                if line.find("Opening archive <2 ") >=0 and line.find("> with copy = "+copyprecedence) and line.find(str(jobId)) >=0:
                    fd.close()
                    log.info("Found [Opening archive <2 ","> with copy = " + copyprecedence +"is found in"+ FileName+ "]")
                    log.info("matching line is" +line)
                    return(0, True)
            fd.close()
            log.info(" Opening archive <2 ","> with copy = "+copyprecedence+ " not found in file [" +FileName+ "]")
            return(1, "Not found")
        else:
            log.error("File [" +FileName+ "] not found")
            return(2, "File not Found")
    except:
        log.exception("Exception raised in isRestoreDoneFromSeconadaryCopy")
        return(-1,False)
		
		

