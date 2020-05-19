from AutomationUtils import cmdhelper, loghelper, remoteconnection
from SimpanaUtils import subclienthelper, storagepolicyhelper, clienthelper
import AutomationConstants, traceback
from FSIdaHelper import SnapProtectHelper
##from NASHelper import snaphelperfunctions,snapconstants
import os, sys, cx_Oracle, time
from oraclehelper import oraclehelper
from OperatingSystemUtils.UnixHelper import UnixHelper

def JobManipulation(data, retcode, CommServerName):
    log = loghelper.getLog()
    jobId = cmdhelper.getJobId(data)
    (returnCode, returnString) = cmdhelper.waitForJobToComplete(jobId, CommServerName)
    if returnString != True:
        log.error("There is issue with the job creation & run")
        return -1
    else:
        return jobId

def setup(CurrentRun):
    "Set up function"
    log = loghelper.getLog()
    log.info("This is setup function of %s Testcase" % CurrentRun.getTestCaseName())

def tearDown(CurrentRun):
    "Tear Down function"
    log = loghelper.getLog()
    log.info("This is teardown function of %s Testcase"% CurrentRun.getTestCaseName())

def run(CurrentRun):
    "Run function"
    setup(CurrentRun)
    log = loghelper.getLog()
    try:
        answers = CurrentRun.getAnswers()
        DataAgentType=str(answers['DataAgentType'])
        CommServerName=str(answers['CommServerName'])
        Usr=AutomationConstants.CV_AUTOMATION_USR
        Pwd=AutomationConstants.CV_AUTOMATION_PWD
        DefaultStoragePolicy=str(answers['DefaultStoragePolicy'])
        SnapCopyName="SNAP_COPY"
        ClientName=str(answers['ClientName'])
        ClientHostName=str(answers['ClientHostName'])
        ConnectUser=str(answers['ConnectUser'])
        ConnectPassword=str(answers['ConnectPassword'])
        ConnectString=str(answers['ConnectString'])
        Instance=str(answers['InstanceName'])
        RemoteMachineUserName=str(answers['RemoteMachineUserName'])
        RemoteMachinePassword=str(answers['RemoteMachinePassword'])
        OSType=str(answers['OSType'])
        LocalDBTNSNamesPath=str(answers['LocalDBTNSNamesPath'])
        LogStoragePolicy=str(answers['LogStoragePolicy'])
        CmdStoragePolicy=str(answers['CmdStoragePolicy'])
        CatalogConnectEnable=str(answers['CatalogConnectEnable'])
        CatalogUser=str(answers['CatalogUser'])
        CatalogPassword=str(answers['CatalogPassword'])
        CatalogConnectString=str(answers['CatalogConnectString'])
        OracleUser=str(answers['OracleUser'])
        OracleUserPassword=str(answers['OracleUserPassword'])
        MAName=str(answers['MAName'])
        LibraryName=str(answers['LibraryName'])
        SimpanaInstanceName = str(answers['SimpanaInstance'])
        ResultFlag=False

        SnapVendorName = str(answers['SnapVendor'])
        #SMArrayId =  str(answers['ArrayID'])
        #ControlHost = str(answers['ControlHost'])
        #SMHostUserName = str(answers['ArrayUserName'])
        #SMHostPassword = str(answers['ArrayPassword'])
        #DeviceGroup = str(answers['DeviceGroup'])
        tblCountG=0

        ##testCaseID = CurrentRun.getTestCaseName()
        cmdhelper.cvLogout(CommServerName)
        csn, b, c, d = CommServerName.split(".")
        log.info("CSN Name:" + str(csn))
        (_retcode, _retval) = cmdhelper.cvLogin(CommServerName, Usr, Pwd, str(csn))
        if _retcode != 0:
            log.error("Login Failed")
            return 1
        else:
            ResultFlag=True
            log.info("Done - Login to CommServe")



        log.info("SNAP BACKUP - Creating Storagepolicy if it doesnot exist.")
        retcode = storagepolicyhelper.spExists(CommServerName , DefaultStoragePolicy )
        if retcode == False:
            retcode = storagepolicyhelper.createStoragePolicy(CommServerName, DefaultStoragePolicy, MAName, LibraryName, 5)
            if retcode != True:
                ResultFlag=False
                log.exception("Error creating " + DefaultStoragePolicy +" Storage Policy.")
                raise
            else:
                ResultFlag=True
                log.info("Success - Creating " + DefaultStoragePolicy + " StoragePolicy.")
        else:
            ResultFlag=True
            log.info(" StoragePolicy " + DefaultStoragePolicy + " exists. Skipping...")

        log.info("SNAP BACKUP - Creating SNAP COPY if it does not exist.")
        retcode = storagepolicyhelper.SPCopyExists(CommServerName,DefaultStoragePolicy,SnapCopyName)
        if retcode == False:
            log.info("Creating secondary copy ")
            result = storagepolicyhelper.createSnapCopy(CommServerName,DefaultStoragePolicy,SnapCopyName,MAName,LibraryName)
            if result !=True:
                ResultFlag=False
                log.error("Creating Snap copy failed")
                loghelper.setResultString("Creating Snap copy failed")
                return 4
            else:
                ResultFlag=True
                log.info("Success - Creating " + SnapCopyName + " StoragePolicy.")
        else:
            ResultFlag=True
            log.info(" StoragePolicy " + SnapCopyName + " exists. Skipping...")

        log.info("Make oracle instance entry in tnsnamesora file")
        (retCode, retString) = oraclehelper.UpdateTNSNamesOraFile(LocalDBTNSNamesPath, ClientHostName, ConnectString)
        if retCode == -1:
            raise
        ##connect to DB and create a table
        (retCode, retString, conn, curs) = oraclehelper.connectOracle(ConnectUser, ConnectPassword, ConnectString)
        if retCode == -1:
            log.error("OperationalError: ORA-01034: ORACLE not available")
            raise

        (_retCode, state) = oraclehelper.GetDatabaseState(ConnectUser, ConnectPassword, ConnectString)
        if _retCode != 0 or state != "OPEN":
            log.error(" Database is not in open state.")
            try:
                curs.execute('alter database open')
            except:
                raise

        if OSType.lower() != "windows":
            sshConn=remoteconnection.Connection(ClientHostName, OS="unix", username=RemoteMachineUserName , password=RemoteMachinePassword)
            #SimpanaInstanceName=UnixHelper.getInstanceName(ClientHostName, CommServerName,RemoteMachineUserName, RemoteMachinePassword)
            SimpanaLogDirectory = UnixHelper.getLogDirectoryPath(ClientHostName, SimpanaInstanceName, RemoteMachineUserName, RemoteMachinePassword)
            SimpanaLogDirectory = SimpanaLogDirectory + "/"

        else:
            sshConn=remoteconnection.Connection(ClientHostName, OS="windows", username=RemoteMachineUserName , password=RemoteMachinePassword)
            (retCode, SimpanaLogDirectory) = clienthelper.getSimpanaLogDir(CommServerName, ClientName,"CC")
            SimpanaLogDirectory = SimpanaLogDirectory + "\\"

        (retCode, LocalPath) = oraclehelper.getCommonDirPath()
        retCode = oraclehelper.hasInstance(CommServerName, ClientName, DataAgentType, Instance)
        if retCode == False:
            log.info("Instance doesnot exist. so we need to create ")
            OracleHome = oraclehelper.getOracleHomePath(Instance, ClientHostName, RemoteMachineUserName, RemoteMachinePassword, OSType)
            if OracleHome == 'None':
                raise
            (_retstring, _retval) = oraclehelper.createOracleInstance(CommServerName, ClientName, Instance, DataAgentType, \
                                                                      OracleHome, ConnectUser, ConnectPassword, ConnectString, DefaultStoragePolicy, \
                                                                      LogStoragePolicy, CmdStoragePolicy, CatalogConnectEnable, CatalogUser, CatalogPassword, \
                                                                      CatalogConnectString, OracleUser, OracleUserPassword)

            if(_retstring == None or _retstring == False):
                log.error("Instance creation is failed")
                raise

        log.info("######## SNAP SHOT BACKUP ONLY ########")

        log.info("#### CASE 1: Run SNAP and then run offline backup copy #####")

        log.info("### Enable SNAP Protect at Client Level ###")
        log.info("Client Name:" + ClientName)
        retcode = clienthelper.EnableSnapProtectOnClient(CommServerName, ClientName)
        if retcode != True:
            log.error("Failed to enable Snap Protect on %s" %(ClientName))
            loghelper.setResultString("Failed to enable snapprotect on client")
            return 5

        ###STEP2:CREATE A SUBCLIENT AND FROM THE CONTENT TAB SELECT ONLINE DATABASE,BACKUP ARCHIVE LOG OPTION###
        log.info("###CREATE OF SUBCLIENT WITH PROPERTIES SELECT ONLINE DATABASE AND BACKUP ARCHIVE LOG STARTS HERE###")

        SubClientName="SNAP_OFFLINE_BACKUP_COPY"

        retCode = subclienthelper.hasSubclient(CommServerName, ClientName, DataAgentType,
                                               None, SubClientName, Instance)
        if retCode == False:
            "SNAP BACKUP: Selective Online Full- Subclient doesnot exist so we need to create New Subclient"
            retCode = oraclehelper.CreateOracleSubclient(CommServerName, ClientName, Instance, SubClientName, DefaultStoragePolicy)
            if retCode != True:
                log.error("error while creating Subclient")
                raise

        "SNAP BACKUP: Enable Selective Online Full-"
        (retCode, retString) = oraclehelper.setSubclientPropSelectiveOnlineFullON(CommServerName, ClientName, Instance, SubClientName)
        if(retCode == None or retCode == False):
            log.error("Failed to Enable Selective Online Full property for subclient")
            raise
	
        tblSpaceG="TS_SNAP"
        tblNameG="TBL_SNAP"
        tblCountG+=1
	
	###Creating a tablespace and tables##
	log.info("SNAP - Creating some test data##")
	(retCode, retString, firstrow) = oraclehelper.getDatafile(curs, tblSpaceG, OSType)
	log.info("firstrow we got is " +firstrow)
	if retCode == -1:
		log.error("Database is not in mounted state")
		raise

		
	(_retCode, _retString) = oraclehelper.createTable(curs, firstrow, tblSpaceG, tblNameG+str(tblCountG), True)
	if _retCode !=0:
		log.error("Failed to create table in tablespace")
		raise

        log.info("Enable Snap Protect Details on subclient")

	(retCode, retString) = oraclehelper.EnableSubclientSnapProtectDetails(CommServerName, ClientName, Instance, SubClientName,SnapVendorName)
       # (_retCode,_data)=cmdhelper.executeCommand(r"qoperation execute -af update_subclient_template.xml -appName Oracle -clientName " + ClientName +
                                        #            " -cs " + CommServerName + " -instanceName " + Instance + " -subclientName " + SubClientName +
                                                #    " -isRMANEnableForTapeMovement true " + " -isSnapBackupEnabled true " +
                                             #       " -snapShotEngineName " + SnapVendorName + " -StoragePolicyName " + DefaultStoragePolicy )


        SPCopyID = storagepolicyhelper.getCopyIDBySPandCopyName(DefaultStoragePolicy, SnapCopyName, CommServerName)
        log.info("Secondary copy copy id:%s" %(SPCopyID))

        result = storagepolicyhelper.updateSPCopyRetention(SPCopyID, 1,0, CommServerName)
        log.info("Changed the Retention on Snap Copy to 0 days 1 cycle")


        log.info("Start Backup and wait for the job to complete ")
        #log.info(" [%s] Backup Start for TC - [%s]" %(backupType, testCaseID))
        retCode, data = cmdhelper.executeCommand("qoperation execute -af SNAP_BACKUP.xml" +
                                                             " -cs " +  CommServerName +
                                                             " -clientName " + ClientName +
                                                             " -instanceName " + Instance +
                                                             " -subclientName " + SubClientName +
                                                             " -backupLevel FULL" )
        if retCode != 0:
            log.error("Failed to start backup")
            cmdhelper.cvLogout(CommServerName)
            return 1

        #wait for the job to complete
        log.info("Waiting for Backup job to complete")
        if cmdhelper.waitForJobToComplete(cmdhelper.getJobId(data),CommServerName) != (0, True):
            log.info("Backup Job failed to complete. Script exiting")
            cmdhelper.cvLogout(CommServerName)
            return 1

        #log.info("[%s] Backup Job completed successfully" %backupType)
        log.info("SNAP BACKUP COPY - Running offline SNAP Backup copy.")
        #log.info(" [%s] Backup Start for TC - [%s]" %(backupType, testCaseID))
        (_retCode,_data) = cmdhelper.executeCommand("qoperation execute -af BACKUP_COPY.xml" + " -cs " +  CommServerName + " -storagePolicyName " + DefaultStoragePolicy )

        #wait for the job to complete
        log.info("Waiting for Backup job to complete")
        if _retCode == -1:
            log.error("Failed to start qoperation backup operation")
            raise
        jobId=JobManipulation(_data, _retCode, CommServerName)
        if jobId == -1:
            raise

        CommServerTimeZone = oraclehelper.getCommServerTimeZone(CommServerName)
        ####Submitting restore job from SNAP backup###
        log.info("SNAP - Submitting restore job from SNAP BACKUP ###")
        (_retCode,_data)=cmdhelper.executeCommand(r"qoperation execute -af RESTORE_FROM_SNAP.xml -clientName " + \
                                                  ClientName + " -cs " + CommServerName + " -instanceName " + Instance + " -oracleOpt/catalogConnect1 " + CatalogUser + " -oracleOpt/catalogConnect2/password " + \
							CatalogPassword+ " -oracleOpt/catalogConnect3 " + CatalogConnectString + " -TimeZoneName " + "\"" + CommServerTimeZone + "\"")

        #wait for the job to complete
        log.info("Waiting for Backup job to complete")
        if _retCode == -1:
            log.error("Failed to start qoperation backup operation")
            raise
        jobId=JobManipulation(_data, _retCode, CommServerName)
        if jobId == -1:
            raise
        else:
            ResultFlag=True
        log.info("SNAP BACKUP - Submitting another SNAP backup to REVERT from this SNAP BACKUP ###")
        retCode, data = cmdhelper.executeCommand("qoperation execute -af SNAP_BACKUP.xml" +
                                                             " -cs " +  CommServerName +
                                                             " -clientName " + ClientName +
                                                             " -instanceName " + Instance +
                                                             " -subclientName " + SubClientName +
                                                             " -backupLevel FULL" )
        if retCode != 0:
            log.error("Failed to start backup")
            cmdhelper.cvLogout(CommServerName)
            return 1

        #wait for the job to complete
        log.info("Waiting for Backup job to complete")
        if cmdhelper.waitForJobToComplete(cmdhelper.getJobId(data),CommServerName) != (0, True):
            log.info("Backup Job failed to complete. Script exiting")
            cmdhelper.cvLogout(CommServerName)
            return 1

        ####Submitting restore job from SNAP backup###
        log.info("SNAP - Submitting REVERT restore job from SNAP BACKUP ###")

	(_retCode,_data)=cmdhelper.executeCommand(r"qoperation execute -af RESTORE_FROM_SNAP.xml -revert true -useRmanRestore true -clientName " + \
						ClientName + " -cs " + CommServerName + " -instanceName " + Instance + " -oracleOpt/catalogConnect1 " + CatalogUser + " -oracleOpt/catalogConnect2/password " + \
							CatalogPassword+ " -oracleOpt/catalogConnect3 " + CatalogConnectString + " -TimeZoneName " + "\"" + CommServerTimeZone + "\"")

        '''(_retCode,_data)=cmdhelper.executeCommand(r"qoperation execute -af RESTORE_FROM_SNAP.xml -revert true -clientName " + \
                                                  ClientName + " -cs " + CommServerName + " -instanceName " + Instance + " -TimeZoneName " + "\"" + CommServerTimeZone + "\"")'''

        #wait for the job to complete
        log.info("Waiting for Backup job to complete")
        if _retCode == -1:
            log.error("Failed to start qoperation backup operation")
            raise
        jobId=JobManipulation(_data, _retCode, CommServerName)
        if jobId == -1:
            raise
        else:
            ResultFlag=True

        #Compare data from restore
        log.info("Data Comparison - Start.")

        #log.info("TEST CASE - [%s] - [%s] FINISHED SUCCESSFULLY." %(testCaseID, testCaseName))

        cmdhelper.cvLogout(CommServerName)
        #oraclehelper.removeCommonDir()
        if ResultFlag != False:
            log.info("TC 39393-PASSED")
            return 0
        else:
            log.info("TC-39393  - FAILED")
            return 1
    except:
        log.exception("Exception in testcase-run")
        log.error("Exception Raised. Execution of Test Case [%s] Failed." %testCaseID)
        log.error(traceback.format_exc().splitlines())
        return 1
    finally:
        tearDown(CurrentRun)

if __name__ == "__main__":
    run()
