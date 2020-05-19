#!/usr/bin/env python
#############################################################################################################
# Name          : cvepmigration.py
# Created By    : Anirudh S Narayan, Shankar Vullupala
#
# Description   : Class that does the actual migration
# Notes         :
#############################################################################################################
try:
    import itertools
    import glob
    import operator
    import logging
    import shutil
    import sys
    import os
    import subprocess
except ImportError:
    print('Unable to import modules in cvepmigration')
    raise

try:
    from cvhelper import *
except ImportError:
    print('Unable to import modules in cvhelper')
    raise

class CvOraExport(Cvhelper):

    """Docstring for CvExport. """

    def __init__(self, db):
        """TODO: to be defined1. """
        try:
            Cvhelper.__init__(self, db)
        except:
            raise

    def preChecks(self):
        """TODO: Docstring for createDDLImagecopy.
        :returns: TODO

        """
        try:
            print('--------------validate the given paths and dump files--------------------')
            filepaths = [self.cv_oraenv['imagecopy_path'],self.cv_oraenv['Datafiles_path'], self.cv_oraenv['dest_path'], self.cv_oraenv['Trans_path']]
            for i in filepaths:
                if os.path.isdir(i):
                    print(i +' path validated')
                    logging.info(i + ' path validated')
                else: 
                    raise cvException('Incorrect '+i+ ' path')
            #Need to honor the files convert_replicadatafiles.rman and tablespace_impdp_replicated.in from NFS&FS backup/restore use case
            #Checking for dump and input files to import sql objects are added sqlobject_export01.dmp , sqlobject_impdp.in .
            scriptDumps=['user_export01.dmp', 'dropTableSpace.sql', 'convert_datafiles.rman', 'tablespace_impdp.in', 'tablespace_export01.dmp', 'tableSpaceList.txt',
                         'sqlobject_export01.dmp','sqlobject_impdp.in']
            for i in scriptDumps:
                if os.path.isfile(os.path.join(self.cv_oraenv['Trans_path'],i)):
                    print(i+' exists')
                    logging.info(i + ' exists')
                else: 
                    print("The requested file "+i+" not found")
                    logging.warning("The requested file "+i+" not found")
                    if i is 'convert_datafiles.rman':
                        if os.path.isfile(os.path.join(self.cv_oraenv['Trans_path'] ,'convert_replicadatafiles.rman')):
                            logging.info("convert_replicadatafiles.rman exists and convert_datafiles.rman do not exist")
                        else:
                            raise cvException('Both convert_replicadatafiles.rman or convert_datafiles.rman do not exist')
                    elif i is 'tablespace_impdp.in':
                        if os.path.isfile(os.path.join(self.cv_oraenv['Trans_path'],'tablespace_impdp_replicated.in')):
                            logging.info("tablespace_impdp_replicated.in exists and tablespace_impdp.in do not exist")
                        else:
                            raise cvException('Both tablespace_impdp.in or tablespace_impdp_replicated.in do not exist')
                    else:
                        raise cvException('Error in fetching the required file')

            print('------------Validating free space--------------')

            p = subprocess.check_output('du -h '+self.cv_oraenv['Datafiles_path'], shell=True)
            t = ''
            for i in range(0,len(p)):
                if p[i] == '/':
                    break
                else:
                    t = t+p[i]
            
            print('Space used by dumped datafiles: '+t)
            logging.warning('Space used by dumped datafiles: ' + t)

            p = subprocess.check_output('df -h '+self.cv_oraenv['dest_path'], shell=True)
            tempdf = []
            for i in p.split(' '):
                if i != '':
                    tempdf.append(i)
            print('Space available in disk of the destination path : '+tempdf[-3])
            logging.warning('Space available in disk of the destination path : ' + tempdf[-3])
            
            print('----------------------------------------------')
            p = subprocess.check_output('ls -ltr '+self.cv_oraenv['dest_path'], shell=True)
            print('Files at destination: '+p)
            logging.info('Files at destination: ' + p)
            logging.warning('Please check for the datafiles to avoid overwriting')

            op = self.execSQL("CREATE OR REPLACE directory TRANSPORT_METADATA as " + "'"+self.cv_oraenv['Trans_path']+"'" , True)
            logging.info('DB directory: ' + str(flatten(op)))

            print('--------------DB status------------')
            env=self.getOraEnv()
            print('DB used for pumping data is '+env['ORACLE_SID'])
            logging.info('DB used for pumping data is ' + env['ORACLE_SID'])

            if self.dbStatus() == 'OPEN':
                print ('DB Mode is OPEN, so proceeding further')
                logging.info('DB Mode is OPEN, so proceeding further')
            else: 
                print ('selected DB not in open mode to proceed further')
                logging.info('selected DB not in open mode to proceed further')
                raise cvException('selected DB not in open mode to proceed further')
            return True
            
        except:
            e = sys.exc_info()[1]
            logging.error(e.args[0])
            raise cvException('errors in preChecks')

    def generateDDL(self):
        """TODO: Docstring for createDDLImagecopy.
        :returns: TODO

        """
        try:
            logging.info("Generate users.sql")
            print("--------------Generate users.sql------------------")
            
            usr = self.cv_oraenv['remote_user']
            pwd = self.cv_oraenv['remote_passwd']
            db = self.cv_oraenv['remote_db']

            sch = ' '
            if not usr:
                logging.info('Using / connect')
                sch = "\'/ as sysdba\' directory=TRANSPORT_METADATA DUMPFILE=user_export01.dmp INCLUDE=user SQLFILE=users.sql"

            else:
                logging.info(usr+'/xxxx@'+db+' connect string')
                sch = '\"'+usr+'/'+pwd+'@'+db+str(' as sysdba\"')+str(' directory=TRANSPORT_METADATA DUMPFILE=user_export01.dmp INCLUDE=user SQLFILE=users.sql')
            #logging.info(str(sch))
            op = self.importData(sch)
            logging.info("Import data output:" + str(op)) 
            for i in op['output']: 
                print (str(i))
                logging.info(str(i))
            if op['return_code'] != 0:
                logging.warning('users generation unsuccessfull')
                print('-------users generation unsuccessfull-------')
                raise cvException('Error in generating users.sql.')
            else:
                logging.info('users generation Successfull')
                print('users generation Successfull')
                return True  
        except:
            e = sys.exc_info()[1]
            logging.error(e.args[0])
            raise cvException('Error in generating DDL file and editing it')


    def createDDLImgCpy(self):
        """TODO: Docstring for createDDLImagecopy.
        :returns: TODO

        """
        try:
            if self.generateDDL():
                logging.info('-------making copy of the generated users.sql-------')
                if os.path.isfile(os.path.join(self.cv_oraenv['Trans_path'],'users.sql')):
                    print('users.sql exists in the required path')
                    logging.info('users.sql exists in the required path')
                else:
                    logging.info('users.sql do not exists in the required path')
                    raise cvException('Could not find the users.sql to create users in the DB')
                srcpath=os.path.join(self.cv_oraenv['Trans_path'],'users.sql')
                despath=os.path.join(self.cv_oraenv['Trans_path'],'copy_of_users.sql')
                shutil.copyfile(srcpath,despath)
                logging.info('Made a copy of generated users.sql file')
                tbscp_list=['USERS']
                with open(os.path.join(self.cv_oraenv['Trans_path'],'tableSpaceList.txt'), 'r') as file:
                    for line in file.read().splitlines():
                        tbscp_list.append(line)
                logging.info(tbscp_list)
                with open(os.path.join(self.cv_oraenv['Trans_path'],'users.sql'), 'r+') as file:
                    filedata = file.readlines()
                file.close()
                with open(os.path.join(self.cv_oraenv['Trans_path'],'users.sql'), 'w') as file:
                    for line in filedata:
                        if line.find('DEFAULT TABLESPACE')>0:
                            file.write(';\n')
                        elif line.find('TEMPORARY TABLESPACE')>0:
                            continue
                        else:
                            file.write(line)
                file.close()
                
            logging.info("Finished editing users.sql")
            print("----------------Executing users.sql---------------")
            op = self.execSQL('@' + os.path.join(self.cv_oraenv['Trans_path'],'users.sql'), True)
            logging.info('Output for running the modified users script: ' + str(flatten(op)))
            for i in flatten(op): 
                print(str(i))
                logging.info(str(i))
            self.dropTSImgCpy()
            return True

        except:
            e = sys.exc_info()[1]
            logging.error(e.args[0])
            raise cvException('Error in handling users sql')

    def dropTSImgCpy(self):
        """TODO: Docstring for dropTSImgCpy.
        :returns: TODO

        """
        try:
            logging.info("Dropping tablepsaces using dropTableSpace.sql")
            print("--------Dropping tablepsaces using dropTableSpace.sql--------------")

            op = self.execSQL('@' + os.path.join(self.cv_oraenv['Trans_path'],'dropTableSpace.sql'), True)
            logging.info('Output running the dropTableSpace.sql script: ' + str(flatten(op)))
            for i in flatten(op): 
                print(str(i))
                logging.info(str(i))
            self.processTS()
        except:
            e = sys.exc_info()[1]
            logging.error(e.args[0])
            raise cvException('Error in deleting the ts')

    def rmanConvertImgCpy(self):
        """TODO: Docstring for rmanConvertImgCpy.
        :returns: TODO
        """
        try:
            #Checking for /convert_replicadatafiles.rman
            if os.path.isfile(os.path.join(self.cv_oraenv['Trans_path'],'convert_replicadatafiles.rman')):
                userman = 'convert_replicadatafiles.rman'
            else:
                userman = 'convert_datafiles.rman'

            logging.info("started editing "+userman)
            print("-----------Executing RMAN convert data files-------------")
            usr = self.cv_oraenv['remote_user']
            pwd = self.cv_oraenv['remote_passwd']

            #Modifyng the rman convert script for paths
            with open(os.path.join(self.cv_oraenv['Trans_path'],userman), 'r') as file:
                filedata = file.read()
            # Replace the target string

            filedata = filedata.replace('IMAGECOPY_PATH/', str(self.cv_oraenv['Datafiles_path'])+str('/'))
            filedata = filedata.replace('TARGET_PATH/', str(self.cv_oraenv['dest_path'])+str('/'))

            logging.info("Replaced IMAGECOPY_PATH and TARGET_PATH in " +str(userman))

            with open(os.path.join(self.cv_oraenv['Trans_path'],userman), 'w') as file:
                file.write(filedata)
            file.close()

            #rman target sys/sys cmdfile userman
            sch = 'target '+usr+'/'+pwd+ ' cmdfile '+ str(os.path.join(self.cv_oraenv['Trans_path'], userman))+' log='+str(os.path.join(self.cv_oraenv['Trans_path'],'rman_script.log'))
            #logging.info(str(sch))
            op = self.rmanExec(sch)
            logging.info('RMAN Command output: ' + str(op))
            for i in op['output']: 
                print (str(i))
            if op['return_code'] != 0:
                logging.info('RMAN Command output: ' + str(op))
                with open(os.path.join(self.cv_oraenv['Trans_path'],'rman_script.log'), 'r') as file:
                    for line in file: 
                        if 'RMAN-' in line or 'ORA-' in line: 
                            print(line)
                            logging.info(line)
                file.close()
                raise cvException('Error code from RMAN Command.')
            else: 
                logging.info("Completed executing "+str(userman))
                if self.importTSDumpImgCpy():
                    return True
                else:
                    raise cvException('Error in importing tablespaces')

        except:
            e = sys.exc_info()[1]
            logging.error(e.args[0])
            raise cvException('Error in modifying and executing rman script')

    def importTSDumpImgCpy(self):
        """TODO: Docstring for importTSDumpImgCpy.
        :returns: TODO

        """
        try:
            # Checking for /tablespace_impdp_replicated.in
            if os.path.isfile(os.path.join(self.cv_oraenv['Trans_path'],'tablespace_impdp_replicated.in')):
                usertbls = 'tablespace_impdp_replicated.in'
            else:
                usertbls = 'tablespace_impdp.in'
            logging.info("Editing " +str(usertbls))
            print("----------------Importing tablespaces-------------")
            
            with open(os.path.join(self.cv_oraenv['Trans_path'],usertbls), 'r') as file:
                filedata = file.read()

            filedata = filedata.replace('TARGET_PATH/', str(self.cv_oraenv['dest_path'])+str('/'))
            logging.info("Replaced the 'TARGET_PATH' to the required locaiton")

            with open(os.path.join(self.cv_oraenv['Trans_path'] ,usertbls), 'w') as file:
                file.write(filedata)

            file.close()
            usr = self.cv_oraenv['remote_user']
            pwd = self.cv_oraenv['remote_passwd']
            db = self.cv_oraenv['remote_db']
            logging.info("Executing " +str(usertbls))
            sch = ' '

            if not usr:
                logging.info('Using / connect')
                sch = "\'/ as sysdba\'"+str(' PARFILE=')+str(os.path.join(self.cv_oraenv['Trans_path'] ,usertbls))
            else:
                logging.info(usr + '/xxxx@' + db + ' connect string')
                sch = '\"'+usr+'/'+pwd+'@'+db+str(' as sysdba\"')+str(' PARFILE=')+str(os.path.join(self.cv_oraenv['Trans_path'] ,usertbls))
            #logging.info(str(sch))
            op = self.importData(sch)
            logging.info("Import data output:" + str(op))
            for i in op['output']: 
                print (str(i))
                logging.info(str(i))
            if op['return_code'] != 0:
                logging.warning('Import of tabelspaces not successful. Please check and rerun.')
            else:
                logging.info("Import of tabelspaces is successful")
            if self.addGrantsImgCpy():
                return True
        except:
            e = sys.exc_info()[1]
            logging.error(e.args[0])
            raise cvException('Error thrown by metadata import.')

    def addGrantsImgCpy(self):
        """TODO: Docstring for addGrantsImgCpy.
        :returns: TODO

        """
        try:
            with open(os.path.join(self.cv_oraenv['Trans_path'],'copy_of_users.sql'), 'r') as file:
                filedata = file.read()
            filedata = filedata.replace('CREATE', 'ALTER')
            with open(os.path.join(self.cv_oraenv['Trans_path'],'copy_of_users.sql'), 'w') as file:
                file.write(filedata)
            file.close()
            logging.info("Edited users.sql")
            print("------------Importing database user definitions---------")
            op = self.execSQL('@' + str(os.path.join(self.cv_oraenv['Trans_path'],'copy_of_users.sql')), True)
            logging.info('Output for running the modified users script: ' + str(flatten(op)))
            for i in flatten(op): 
                print(str(i))
                logging.info(str(i))
            usr = self.cv_oraenv['remote_user']
            pwd = self.cv_oraenv['remote_passwd']
            db = self.cv_oraenv['remote_db']

            logging.info("Adding role, system and object grants")
            print("-----------Extracting user grants-------------")
            grants = ['role_grant sqlfile=role_grants.sql', 'system_grant sqlfile=system_grants.sql', 'object_grant sqlfile=object_grants.sql']
            sch = ' '
            for grant in grants:
                if not usr:
                    logging.info('Using / connect')
                    sch = "\'/ as sysdba\'"+str(" directory=TRANSPORT_METADATA DUMPFILE=user_export01.dmp include=")+str(grant)
                else:
                    logging.info(usr + '/xxxx@' + db + ' connect string')
                    sch = '\"'+usr+'/'+pwd+'@'+db+str(' as sysdba\"')+str(" directory=TRANSPORT_METADATA DUMPFILE=user_export01.dmp include=")+str(grant)
                #logging.info(str(sch))
                op = self.importData(sch)
                for i in op['output']: 
                    print (str(i))
                    logging.info(str(i))

            print("------------Importing user grants---------")
            grant_sql=['role_grants.sql', 'system_grants.sql', 'object_grants.sql']
            for i in grant_sql:
                if os.path.isfile(os.path.join(self.cv_oraenv['Trans_path'],i)):
                    logging.info(i + ' exists')
                    op = self.execSQL('@' + str(os.path.join(self.cv_oraenv['Trans_path'],i)), True)
                    logging.info('Output for running the '+str(i)+': ' + str(flatten(op)))
                    for i in flatten(op): 
                        print(str(i))
                        logging.info(str(i))
                else:
                    logging.warning("The requested file "+i+" not found")
            logging.info("Completed executing the users.sql and adding grants")

            #import for SQL Objects
            logging.info("Importing SQL Objects")
            print("------------Importing SQL Objects---------")
            sch = ' '
            if not usr:
                logging.info('Using / connect')
                sch = "\'/ as sysdba\'" + str(' PARFILE=') + str(os.path.join(self.cv_oraenv['Trans_path'],'sqlobject_impdp.in'))
            else:
                logging.info(usr + '/xxxx@' + db + ' connect string')
                sch = '\"' + usr + '/' + pwd + '@' + db + str(' as sysdba\"') + str(' PARFILE=') + str(os.path.join(self.cv_oraenv['Trans_path'],'sqlobject_impdp.in'))
            op = self.importData(sch)
            logging.info("Import data output:" + str(op))
            for i in op['output']:
                print(str(i))
                logging.info(str(i))
            return True

        except:
            e = sys.exc_info()[1]
            logging.error(e.args[0])
            raise cvException('Error applying grants')

    def createOraDir(self, dirs):
        """TODO: Docstring for createOraDir.
        :returns: TODO

        """
        op = self.execSQL("create or replace directory cv_export_dir as \'" + dirs + "\'", True)
        logging.info('Directory creation output for ' + dirs + ': ' + str(op))

    def dbStatus(self):
        try:
            op = self.execSQL("select status from v$instance",True)
            logging.info('DB status: ' + str(flatten(op)))
            for i in flatten(op):
                for j in i.split():
                    if j=='OPEN':
                        self.cv_oraenv['db_status']  = 'OPEN'
            return self.cv_oraenv['db_status']
        except: 
            e = sys.exc_info()[1]
            logging.error(e.args[0])
            raise cvException('Error giving db status')

    def processTS(self):
        op=self.execSQL("select tablespace_name from dba_tablespaces where contents='PERMANENT'  and tablespace_name not IN ('SYSTEM', 'SYSAUX', 'EXAMPLE')", True)
        self.cv_oraenv['tslist']=flatten(op)
        logging.info('TS List: '+str(op))

    def markTSRO(self):
        """TODO: Docstring for markTSRO.
        :returns: TODO

        """
        ts='('
        for a in self.cv_oraenv['tslist']:
            op=self.execSQL("alter tablespace "+a+" read only", True)
            ts+='\''+a+'\','
            logging.info('TS RO output for '+a+": "+str(op))
        ts=ts[:-1]
        ts+=')'
        op=self.execSQL("select  'cv_rman_'||t.name||'_'||f.file#||'.bck,'||f.name from v$tablespace t inner join v$datafile f on f.ts#=t.ts# where t.name in"+ts, True)
        try:
            with open(self.cv_oraenv['location']+'/map.txt', 'w') as f:
                for a in op:
                    for b in a:
                        f.write(b+'\n')
        except:
            logging.error('Error at writing map.txt: '+str(op))
            raise


    def markTSRW(self, *ts):
        """TODO: Docstring for markRSRW.
        :returns: TODO

        """
        for a in self.cv_oraenv['tslist']:
            op=self.execSQL("alter tablespace "+a+" read write", True)
            logging.info('TS RW output for '+a+": "+str(op))



    def getSourceTS(self):
        """TODO: Docstring for getSourceTS.
        :returns: TODO

        """
        if len(self.cv_oraenv['schemalist']) == 0:
            raise cvException("Schema list has 0 elements")
        _ts=[]
        for a in self.cv_oraenv['schemalist']:
            op=self.execSQL("select distinct tablespace_name from dba_segments where owner=\'"+a+"\'", True)
            _ts.append(op)
        self.cv_oraenv['tslist']=flatten(_ts)
        logging.info("TS details for schema: "+str(self.cv_oraenv['tslist']))


    def getSourceSchemas(self):
        """TODO: Docstring for function.

        :arg1: TODO
        :returns: TODO

        """
        if len(self.cv_oraenv['tslist']) == 0:
            raise cvException("Schema list has 0 elements")
        _ts=[]
        for a in self.cv_oraenv['tslist']:
            op=self.execSQL("select distinct owner from dba_segments where tablespace_name=\'"+a+"\' ", True)
            _ts.append(op)
        self.cv_oraenv['schemalist']=flatten(_ts)
        logging.info("Schema details for TS: "+str(self.cv_oraenv['schemalist']))

    def getSourceUsers(self):
        """TODO: Docstring for getSourceUsers.
        :returns: TODO

        """
        op=self.execSQL("select username from dba_users where username not like \'%SYS%\' and username not in (\'OUTLN\',\'MGMT_VIEW\',\'FLOWS_FILES\',\'DBSNMP\',\'APEX_030200\',\'ORDDATA\',\'ANONYMOUS\',\'XDB\',\'ORDPLUGINS\',\'SI_INFORMTN_SCHEMA\',\'SCOTT\',\'ORACLE_OCM\',\'XS$NULL\',\'BI\',\'PM\',\'MDDATA\',\'IX\',\'SH\',\'DIP\',\'OE\',\'APEX_PUBLIC_USER\',\'HR\',\'SPATIAL_CSW_ADMIN_USR\',\'SPATIAL_WFS_ADMIN_USR\')", True)
        self.cv_oraenv['schemalist']=flatten(op)
        logging.info("Schema details for TS: "+str(self.cv_oraenv['schemalist']))



    def exportSchemaMeta(self):
        """TODO: Docstring for .
        :returns: TODO

        """
        sch="\'/ as sysdba\' directory=CV_EXPORT_DIR reuse_dumpfiles=Y dumpfile=cv_exp_meta.dmp logfile=cv_exp_meta.log content=metadata_only schemas="
        sch+=','.join(self.cv_oraenv['schemalist'])
        try:
            logging.info('Starting metadata export with options: '+sch)
            op=self.exportData(sch)
            logging.info("Export data output:"+str(op))
            if op['return_code']!=0:
                logging.warning('Export did not complete successfully. Raising error.')
                raise cvException('Error thrown by metadata export.')
        except:
            raise

    def validateFile(self, fname):
        """TODO: Docstring for validateFile.
        :returns: TODO

        """
        if not os.path.isfile(fname):
            raise cvException('Mentioned File List does not exist.')
        if os.stat(fname).st_size==0:
            raise cvException('Mentioned File List is empty.')

    def setFileList(self, str):
        """TODO: Docstring for setFileList.
        :returns: TODO

        """
        try:
            self.validateFile(str)
            self.cv_oraenv['Filelist']=str
        except:
            raise
        

    # TODO: Triggers and constraints are excluded -- check with Brahma
    def importSchemaMeta(self):
        """TODO: Docstring for .
        :returns: TODO

        """
        sch="\'/ as sysdba\' directory=CV_EXPORT_DIR dumpfile=cv_exp_meta.dmp logfile=cv_exp_meta_imp.log EXCLUDE=TABLE sqlfile=schema_meta.sql schemas="
        sch+=','.join(self.cv_oraenv['schemalist'])
        if self.cv_oraenv['export_mode'] == 'transport_tablespaces':
            remap_ts = ' REMAP_TABLESPACE='
            remap_ts += ':USERS_OLD,'.join(self.cv_oraenv['tslist'])
            remap_ts +=':USERS_OLD'
            sch+=remap_ts
        try:
            logging.info('Starting metadata import with options: '+sch)
            op=self.importData(sch)
            logging.info("Import meta data ut:"+str(op))
            # if op['return_code']!=0:
                # logging.warning('Export did not complete successfully. Raising error.')
                # raise cvException('Error thrown by metadata export.', op['return_code'])
        except:
            raise



    def exportSchema(self):
        """TODO: Docstring for exportSchema.
        :returns: TODO

        """
        sch="\'/ as sysdba\' directory=CV_EXPORT_DIR reuse_dumpfiles=Y dumpfile=cv_exp.dmp log=cv_exp.log schemas="
        self.createOraDir(self.cv_oraenv['location'])
        sch+=','.join(self.cv_oraenv['schemalist'])
        try:
            logging.info('Starting export with options: '+sch)
            op=self.exportData(sch)
            logging.info("Export data output:"+str(op))
            self.cleanupPhase()
            if op['return_code']!=0:
                logging.warning('Export did not complete successfully. Raising error.')
                raise cvException('Export schema failed with error.')
        except:
            raise


    def rmanTSBackup(self):
        """TODO: Docstring for rmanTSBackup.
        :returns: TODO

        """
        allocatestr = ''
        for i in range(1, len(self.cv_oraenv['tslist'])+1):
            allocatestr += 'allocate channel c' + str(i) +' device type disk format \''+self.cv_oraenv['location']+'/cv_rman_%N_%f.bck\';\n'
        logging.info('Running RMAN backup for convert tablespaces')
        cmd='connect target /;\nrun {\n' + allocatestr +' convert tablespace '
        cmd+=','.join(self.cv_oraenv['tslist'])
        cmd+=' to platform=\"Linux x86 64-bit\" PARALLELISM 10;\n}\n'
        try:
            with open(self.cv_oraenv['location']+'/rman_script.rcv', 'w') as f:
                f.write(cmd)
            op=self.rmanExec('cmdfile='+self.cv_oraenv['location']+'/rman_script.rcv log='+self.cv_oraenv['location']+'/rman_script.log')
            logging.info('RMAN Command output: '+str(op))
            if op['return_code']!=0:
                logging.warning('RMAN did not complete successfully. Raising error.')
                raise cvException('Error code from RMAN Command.')
        except:
            raise



    def exportTS(self):
        """TODO: Docstring for exportTS.
        :returns: TODO

        """
        ts="\'/ as sysdba\' dumpfile=cv_exp.dmp log=cv_exp.log reuse_dumpfiles=Y directory=CV_EXPORT_DIR "
        ts+=self.cv_oraenv['export_mode']+"="
        dirs=self.cv_oraenv['location']
        self.createOraDir(dirs)
        ts+=','.join(self.cv_oraenv['tslist'])
        try:
            if self.cv_oraenv['export_mode']=='transport_tablespaces':
                self.getSourceUsers()
                self.markTSRO()
            else:
                self.getSourceSchemas()
            logging.info('Starting TS export with options: '+ts)
            op=self.exportData(ts)
            logging.info("Export data output:"+str(op))
            if self.cv_oraenv['export_mode']=='transport_tablespaces':
                self.rmanTSBackup()
                self.markTSRW()
            if op['return_code']!=0:
                logging.warning('Export did not complete successfully. Raising error.')
                raise cvException('Error code from TS export.')
        except:
            logging.warning('Error thrown at backup pahse. Marking TS RW.')
            if self.cv_oraenv['export_mode']=='transport_tablespaces':
                logging.warning('Export did not complete successfully. Tablespaces are stil in Read-Only.')
                #self.markTSRW()
            raise

    def prepareTS(self):
        """TODO: Docstring for prepareTS.
        :returns: TODO

        """
        strng=''
        for a in self.cv_oraenv['tslist']:
            # strng+="create tablespace "+a+" datafile '/3pardata/oradata/xpprim01/data/"+a+"01.dbf' size 10m autoextend on maxsize 100 m;\n"
            strng+="create tablespace "+a+";\n"
        try:
            with open(self.cv_oraenv['location']+'/preparemeta.sql', 'w') as f:
                f.write(strng)
        except:
            raise



    def createSourceTS(self):
        """TODO: Docstring for createRemoteTS.
        :returns: TODO

        """
        op=self.execSQL('@'+self.cv_oraenv['location']+'/preparemeta.sql', True)
        logging.info('Output for creating source TS: '+str(op))


    def createRemoteTS(self):
        """TODO: Docstring for createRemoteTS.
        :returns: TODO

        """
        usr=self.cv_oraenv['remote_user']
        pwd=self.cv_oraenv['remote_passwd']
        db=self.cv_oraenv['remote_db']
        op=self.execSQL('@'+self.cv_oraenv['location']+'/preparemeta.sql', user=usr, password=pwd, db=db)
        logging.info('Output for creating remote TS: '+str(op))


    def getTTSFiles(self):
        """TODO: Docstring for getTTSFiles.
        :returns: TODO

        """
        try:
            with open(self.cv_oraenv['Filelist'], 'r') as f:
                dirs=f.readlines()
            dirs=[x for x in dirs if len(x)!=0]
            return dirs
        except IOError:
            logging.warning('Error raised at getTTSFiles. Closing Filelist.txt')
            raise

    # TODO: Complete this
    def getSourceDetails(self):
        """TODO: Docstring for getSourceVersion.
        :returns: TODO

        """
        op=self.execSQL("select d.name, d.open_mode, d.log_mode,decode(p.value,'TRUE','RAC','NON-RAC') from v$database d, v$parameter p where p.name='cluster_database'", True)
        logging.info('Source DB Details: '+str(op))
        op=self.execSQL("select value from v$parameter where lower(name)='compatible'", True)
        self.cv_oraenv['version']=op[0][0]
        logging.info('Source DB Version: '+str(op))
        op=self.execSQL("select value from nls_database_parameters where lower(parameter) = 'nls_characterset'", True)
        self.cv_oraenv['character_set']=op[0][0]
        logging.info('Source DB Character Set: '+str(op))

    def dumpSourceMeta(self):
        """TODO: Docstring for dumpSourceMeta.
        :returns: TODO

        """
        try:
            with open(self.cv_oraenv['location']+'/metadata.txt','w') as f:
                f.write("characterset,"+self.cv_oraenv['character_set']+'\n')
                f.write("version,"+self.cv_oraenv['version']+'\n')
        except IOError:
            logging.error('Error writing metadata details for cource')
            raise


    # TODO: Complete this
    def getTargetDetails(self):
        """TODO: Docstring for getTargetDetails.
        :returns: TODO

        """
        usr=self.cv_oraenv['remote_user']
        pwd=self.cv_oraenv['remote_passwd']
        db=self.cv_oraenv['remote_db']
        op=self.execSQL("select d.name, d.open_mode, d.log_mode,decode(p.value,'TRUE','RAC','NON-RAC') from v$database d, v$parameter p where p.name='cluster_database'", user=usr, password=pwd, db=db)
        logging.info('Remote DB Details: '+str(op))
        op=self.execSQL("select value from v$parameter where lower(name)='compatible'", user=usr, password=pwd, db=db)
        self.cv_oraenv['remote_version']=op[0][0]
        logging.info('Remote DB Version: '+str(op))
        op=self.execSQL("select value from nls_database_parameters where lower(parameter) = 'nls_characterset'", user=usr, password=pwd, db=db)
        self.cv_oraenv['remote_character_set']=op[0][0]
        logging.info('Source DB Character Set: '+str(op))


    # TODO: Complete this
    def checkSourceTS(self):
        """TODO: Docstring for checkSourceTS(.
        :returns: TODO

        """
        op=self.execSQL("exec sys.dbms_tts.transport_set_check('"+','.join(self.cv_oraenv['tslist'])+"', true)", True)
        logging.info("TTS set check output for "+str(self.cv_oraenv['tslist'])+': '+str(op))
        op=self.execSQL("SELECT * FROM TRANSPORT_SET_VIOLATIONS", True)
        logging.info("TS Self-containment violations output: " + str(op))
        if len(op) > 0:
            raise cvException("Detected self-containment errors.")


    def importSchema(self):
        """TODO: Docstring for importData.
        :returns: TODO

        """
        logging.info('Starting import of Schemas...')
        if self.cv_oraenv['terminate_cond']=='complete':
            usr=self.cv_oraenv['remote_user']
            pwd=self.cv_oraenv['remote_passwd']
            db=self.cv_oraenv['remote_db']
            opt=usr+'/'+pwd+'@'+db+' dumpfile=cv_exp_totgt.dmp logfile=cv_exp_imp.log directory=\'DATA_PUMP_DIR\' '
        if self.cv_oraenv['terminate_cond']=='import':
            opt='\'/ as sysdba\' dumpfile=cv_exp.dmp logfile=cv_exp_imp.log directory=\'CV_EXPORT_DIR\' '
        sch='schemas='
        for a in self.cv_oraenv['schemalist']:
            sch+='\''+a+'\','
        sch=sch[:-1]
        try:
            op=self.importData(opt+sch)
            logging.info('Import schema output '+str(op))
            if op['return_code']!=0:
                logging.warning('Import did not complete successfully. Raising error.')
                raise cvException('Error code from Schema import.', op['return_code'])
        except:
            raise

        
    def importTS(self):
        """TODO: Docstring for importTS.
        :returns: TODO
        """
        logging.info('Starting import of Tablespaces...')
        if self.cv_oraenv['terminate_cond']=='complete':
            usr=self.cv_oraenv['remote_user']
            pwd=self.cv_oraenv['remote_passwd']
            db=self.cv_oraenv['remote_db']
        # TODO: Add code to include TRANSPORT_DATAFILES when tts is used
        self.createOraDir(self.cv_oraenv['location'])
        if self.cv_oraenv['terminate_cond']=='import':
            opt='\'/ as sysdba\' dumpfile=cv_exp.dmp logfile=cv_exp_imp.log directory=\'CV_EXPORT_DIR\' '
        else:
            opt=usr+'/'+pwd+'@'+db+' dumpfile=cv_exp_totgt.dmp logfile=cv_exp_imp.log directory=\'DATA_PUMP_DIR\' table_exists_action=replace'
        if self.cv_oraenv['export_mode']=='tablespaces':
            sch=' tablespaces='
            for a in self.cv_oraenv['tslist']:
                sch+='\''+a+'\','
        if self.cv_oraenv['export_mode']=='transport_tablespaces':
            sch='EXCLUDE=SCHEMA:"IN (\'SCOTT\')" transport_datafiles='
            flist=self.getTTSFiles()
            for a in flist:
                sch+='\''+a.strip('\n')+'\','
        sch=sch[:-1]
        try:
            logging.info('Using parameters: '+opt+sch)
            op=self.importData(opt+sch)
            logging.info('Import TS output '+str(op))
            if op['return_code']!=0:
                logging.warning('Import did not complete successfully. Raising error.')
                raise cvException('Error code from TS import.', op['return_code'])
        except:
            raise

    def createSourceUsers(self):
        """TODO: Docstring for createUsers.
        :returns: TODO

        """
        loc = os.path.join(self.cv_oraenv['location'],'schema_meta.sql')
        op=self.execSQL(r'@'+loc, True)
        logging.info('Create source user output: '+str(op))

    def createRemoteUsers(self):
        """TODO: Docstring for createUsers.
        :returns: TODO

        """
        usr=self.cv_oraenv['remote_user']
        pwd=self.cv_oraenv['remote_passwd']
        db=self.cv_oraenv['remote_db']
        op=self.execSQL("@"+self.cv_oraenv['location']+"/schema_meta.sql", user=usr, password=pwd, db=db)
        logging.info('Create remote user output: '+str(op))

    def getRemoteDirectory(self):
        """TODO: Docstring for getRemoteDirectory.
        :returns: TODO

        """
        usr=self.cv_oraenv['remote_user']
        pwd=self.cv_oraenv['remote_passwd']
        db=self.cv_oraenv['remote_db']
        op=self.execSQL("select DIRECTORY_PATH from dba_directories where DIRECTORY_NAME=\'DATA_PUMP_DIR\'", user=usr, password=pwd, db=db)
        logging.info('Get remote directory output: '+str(op))
        return op
        

    def importPhase(self, isfull=False):
        """TODO: Docstring for importPhase.
        :returns: TODO

        """
        logging.info('Env variables: '+str(self.getEnv()))
        if self.cv_oraenv['export_mode'] == 'schemas':
            if self.cv_oraenv['terminate_cond']=='import':
                self.createSourceTS()
            if self.cv_oraenv['terminate_cond']=='complete':
                self.prepareTS()
                self.createRemoteTS()
            self.importSchema()
        if self.cv_oraenv['export_mode']=='tablespaces':
            if self.cv_oraenv['terminate_cond']=='import':
                if not isfull:
                    self.createSourceTS()
                self.createSourceUsers()
            if self.cv_oraenv['terminate_cond']=='complete':
                self.prepareTS()
                self.createRemoteTS()
                self.createRemoteUsers()
            self.importTS()
        if self.cv_oraenv['export_mode']=='transport_tablespaces':
            if self.cv_oraenv['terminate_cond']=='import':
                if not isfull:
                    self.createSourceTS()
                self.createSourceUsers()
            self.importTS()
         


    def createDBLink(self):
        """TODO: Docstring for createDBLink.
        :returns: TODO

        """
        usr=self.cv_oraenv['remote_user']
        pwd=self.cv_oraenv['remote_passwd']
        db=self.cv_oraenv['remote_db']
        try:
            op=self.execSQL("create database link to_tgt connect to "+usr+" identified by "+pwd+" using \'"+db+"\'", True)
            #Hiding passwords
            # logging.info('Create DB link output: '+str(op))
            out = []
            for i in op:
                for j in i:
                    if 'identified by' not in j:
                        out.append(i)
            logging.info('Create DB link output: ' + str(out))
        except:
            raise
        
    def transferDumpFiles(self):
        """TODO: Docstring for transferFiles.
        :returns: TODO

        """
        try:
            op=self.execSQL("exec DBMS_FILE_TRANSFER.PUT_FILE(source_directory_object=>\'CV_EXPORT_DIR\',source_file_name=>\'cv_exp.dmp\',destination_directory_object=>\'DATA_PUMP_DIR\',destination_file_name=>\'cv_exp_totgt.dmp\',destination_database=>\'to_tgt\')", True)
            logging.info('Transfer files output: '+str(op))
        except:
            raise
       
    def transferBackupFiles(self):
        """TODO: Docstring for transferBackupFiles.
        :returns: TODO

        """
        try:
            fnames=glob.glob(self.cv_oraenv['location']+'/cv_rman_*.bck')
            for a in fnames:
                op=self.execSQL("exec DBMS_FILE_TRANSFER.PUT_FILE(source_directory_object=>\'CV_EXPORT_DIR\',source_file_name=>\'"+a+"\',destination_directory_object=>\'DATA_PUMP_DIR\',destination_file_name=>\'cv_exp_totgt.dmp\',destination_database=>\'to_tgt\')", True)
                logging.info('Transfer RMAN backup files output for '+a+': '+str(op))
        except:
            raise

    def transferPhase(self):
        """TODO: Docstring for transferPhase.ferPhasferPhasferPhaferPhas

        :):: TODO
        :returns: TODO

        """
        try:
            if self.cv_oraenv['terminate_cond']=='complete':
                self.createDBLink()
                self.transferDumpFiles()
            else:
                raise cvException('Trying to execute transfer phase for wrong export mode')
        except:
            raise


        
    def cleanupPhase(self):
        """TODO: Docstring for cleanupPhase.
        :returns: TODO

        """
        usr=self.cv_oraenv['remote_user']
        pwd=self.cv_oraenv['remote_passwd']
        db=self.cv_oraenv['remote_db']
        op=self.execSQL("drop database link to_tgt", True)
        logging.info("Delete Db link output: "+str(op))
        op=self.execSQL("exec utl_file.fremove(\'DATA_PUMP_DIR\',\'cv_exp_totgt.dmp\')", user=usr, password=pwd, db=db)
        logging.info("Delete target dumpfile output: "+str(op))


    def prepareMeta(self):
        """TODO: Docstring for prepareMeta.
        :returns: TODO

        """
        if self.cv_oraenv['export_mode']=='schemas':
            self.prepareTS()
        elif self.cv_oraenv['export_mode']=='tablespaces' or self.cv_oraenv['export_mode']=='transport_tablespaces':
            self.exportSchemaMeta()
            self.importSchemaMeta()
        
    def copyFromTo(self, src, tgt):
        """TODO: Docstring for copyFromTo.
        :returns: TODO

        """
        try:
            shutil.copy(src, tgt)
        except shutil.Error as e:
            logging.error('Shutil error copying file: '+str(e))
            raise
        except IOError as e:
            logging.error('Shutil IO Error copying map file: '+str(e))
            raise
        except:
            raise cvException('Unrecognized Error: copying map file')

    def exportPhase(self):
        """TODO: Docstring for exportPhase.
        :returns: TODO

        """
        try:
            if self.cv_oraenv['export_mode']=='schemas':
                self.exportSchema()
            if self.cv_oraenv['export_mode']=='tablespaces':
                self.exportTS()
            if self.cv_oraenv['export_mode']=='transport_tablespaces':
                self.exportTS()
            self.dumpSourceMeta()
        except:
            raise
    
    def validationPhase(self):
        """TODO: Docstring for validationPhase.
        :returns: TODO

        """
        self.validateLocation()
        self.getSourceDetails()
        if self.cv_oraenv['terminate_cond']=='complete':
            self.getTargetDetails()
        if self.cv_oraenv['export_mode']=='schemas':
            self.getSourceTS()
        if self.cv_oraenv['export_mode']=='transport_tablespaces':
            if len(self.cv_oraenv['tslist'])==0:
                raise cvException('TS List is empty.')
            self.checkSourceTS()

    def validateLocation(self):
        """TODO: Docstring for validateLocation.
        :returns: TODO

        """
        if not os.path.isdir(self.cv_oraenv['location']):
            raise cvException('Location provided is not a directory or does not exist.')
        if not os.access(self.cv_oraenv['location'], os.W_OK):
            raise cvException('Location provided does not have write permission for this user.')
        logging.info('Location validation complete')

    def setExportMode(self, **par):
        """TODO: Docstring for setExportMode.
        :returns: TODO

        """
        if par['mode'] is None or par['location'] is None or par['entries'] is None:
            raise ValueError('Invalid parameters passed. ' + str(par))
        self.cv_oraenv['export_mode'] = par['mode']
        self.cv_oraenv['location'] = par['location']
        if par['mode'] == 'schemas':
            self.cv_oraenv['schemalist'] = par['entries']
            self.cv_oraenv['schemalist'] = [item.upper() for item in self.cv_oraenv['schemalist']]
        if par['mode'] == 'tablespaces':
            self.cv_oraenv['tslist'] = par['entries']
            self.cv_oraenv['tslist'] = [item.upper() for item in self.cv_oraenv['tslist']]
        if par['mode'] == 'transport_tablespaces':
            self.cv_oraenv['tslist'] = par['entries']
            self.cv_oraenv['tslist'] = [item.upper() for item in self.cv_oraenv['tslist']]
            if self.cv_oraenv['tslist'][0].lower() == 'full':
                self.processTS()
        #if par['mode'] == 'imageCopy':
            #self.cv_oraenv['allfilelist'] = par['entries']
        if par['mode'] == 'full':
            self.processTS()
        #logging.info('Setting pre-export environment to: ' + str(self.getEnv()))

    def setImageCopyMode(self, **par):
        """TODO: Docstring for setExportMode.
        :returns: TODO

        """
        self.cv_oraenv['imagecopy_path'] = par['imagecopy_path']
        self.cv_oraenv['dest_path'] = par['dest_path']
        #self.cv_oraenv['Trans_path'] = self.cv_oraenv['imagecopy_path'] + str('/TRANSPORT_METADATA')
        #self.cv_oraenv['Datafiles_path'] = self.cv_oraenv['imagecopy_path'] + str('/DATAFILES')
        self.cv_oraenv['Trans_path'] = os.path.join(self.cv_oraenv['imagecopy_path'], 'TRANSPORT_METADATA')
        self.cv_oraenv['Datafiles_path'] = os.path.join(self.cv_oraenv['imagecopy_path'], 'DATAFILES')

        # logging.info('Pre-imageCopy environment is set to: ' + str(self.getEnv()))
        logging.info('Paths taken: ')
        logging.info('Oracle_home: ' + str(self.cv_oraenv['oracle_home']))
        logging.info('imge copy path: ' + str(self.cv_oraenv['imagecopy_path']))
        logging.info('dest_path: ' + str(self.cv_oraenv['dest_path']))
        logging.info('Trans_path: ' + str(self.cv_oraenv['Trans_path']))

    def setTermMode(self, cond):
        """TODO: Docstring for setTermMode.
        :returns: TODO

        """
        self.cv_oraenv['terminate_cond'] = cond

    def setRemoteCreds(self, user, passwd, db):
        """TODO: Docstring for setRemoteCreds.
        :returns: TODO

        """
        if user == None:
            self.cv_oraenv['remote_user'] = ''
        else:
            self.cv_oraenv['remote_user'] = user
        if passwd == None:
            self.cv_oraenv['remote_passwd'] = ''
        else:
            self.cv_oraenv['remote_passwd'] = passwd
        if db == None:
            self.cv_oraenv['remote_db'] = self.cv_oraenv['sid']
        else:
            self.cv_oraenv['remote_db'] = db
        #Masking password
        #logging.info('user: ' + str(self.cv_oraenv['remote_user']) + ',' + 'pwd: ' + str(self.cv_oraenv['remote_passwd']) + ',' + 'db: ' + str(self.cv_oraenv['remote_db']))
        logging.info('user: ' + str(self.cv_oraenv['remote_user']) + ',' + 'pwd: ' + 'xxxxxx' + ',' + 'db: ' + str(self.cv_oraenv['remote_db']))


class CVDbaas(Cvhelper):
    """
    CVDBaas -- Class to export RDS databases to a local location
    """

    # SYS Tablespaces and schemas to exclude for export
    SYS_TABLESPACES = ['SYSTEM', 'SYSAUX', 'EXAMPLE', 'USERS']
    SYS_SCHEMAS = ['OUTLN', 'MGMT_VIEW', 'FLOWS_FILES', 'DBSNMP', 'APEX_030200', 'ORDDATA',
                   'ANONYMOUS', 'XDB', 'ORDPLUGINS', 'SI_INFORMTN_SCHEMA', 'SCOTT', 'ORACLE_OCM',
                   r'XS$NULL', 'BI', 'PM', 'MDDATA', 'IX', 'SH', 'DIP', 'OE', 'APEX_PUBLIC_USER',
                   'HR', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR', 'SYSTEM', 'SYS',
                   'APPQOSSYS', 'CTXSYS']

    # All RDS Specific Constants
    RDS_EXPORT_SCHEMAFILE = r'cv_expdp_schemas.dmp'
    RDS_EXPORT_SCHEMALOGFILE = r'cv_expdp_schemas.log'
    RDS_EXPORT_DB_DIRECTORY = r'CV_EXPDP'
    RDS_EXPORT_DB_LINK = r'TO_RDS'
    # Expdp related parameters
    # EXPDP_PARALLEL_STREAMS = 5  #  Oracle SE Does not support parallel execution. So disabling

    def __init__(self, db):
        """Constructor for the class"""
        try:
            super(CVDbaas, self).__init__(db)
        except Exception:
            raise
        else:
            self.remote_user = None
            self.remote_db = None
            self.remote_password = None
            self.schema_list = None
            self.ts_list = None
            self.remote_schemas = []  # List of all remote RDS schemas to be exported
            self.remote_tablespaces = []  # List of all remote RDS tablespaces to be exported

    def set_remote_credentials(self, user, db, passwd):
        """

        Args:
            user (str): string to set RDS user that has DBA privileges
            db (str): string to set the connect identifier to
            passwd (str): string to set the password to
        """
        self.remote_user = user
        self.remote_db = db
        self.remote_password = passwd
        #logging.info('Setting user, db and password to {0} {1} {2}'.format(self.remote_user,self.remote_db,self.remote_password))
        #Hiding passwords
        logging.info('Setting user, db and password to {0} {1} {2}'.format(self.remote_user, self.remote_db,'xxxxx'))

    def _get_sys_schemas(self):
        """

        Returns: formatted comma-separated schemas string with quotes

        """
        return "'{0}'".format("','".join(map(str, CVDbaas.SYS_SCHEMAS)))

    def _get_sys_tablespaces(self):
        """

        Returns: formatted comma-separated tablespaces string with quotes

        """
        return "'{0}'".format("','".join(map(str, CVDbaas.SYS_TABLESPACES)))

    def _get_remote_schemas(self):
        """
        Method to set the instance parameter remote_schemas with a list of all schemas to be
        exported from RDS
        """
        op = self.execSQL("select username from dba_users where username not in ({0})".format(
                self._get_sys_schemas()),
                          False,
                          user = self.remote_user,
                          password = self.remote_password, db = self.remote_db)
        logging.info('Remote DB Non-SYS Schemas: {0}'.format(op))
        for a in op:
            for i in a:
                self.remote_schemas.append(i)

    def _get_remote_tablespaces(self):
        """
        Method to get a list of remote non-system tablespaces.
        """
        op = self.execSQL("select tablespace_name from dba_tablespaces where "
                          "contents='PERMANENT' and "
                          "tablespace_name not in ({0})".format(self._get_sys_tablespaces()),
                          False,
                          user = self.remote_user,
                          password = self.remote_password, db = self.remote_db)
        logging.info('Remote DB Non-SYS Tablespaces: {0}'.format(op))
        for a in op:
            for i in a:
                self.remote_tablespaces.append(i)

    def _remote_schema_export(self):
        """
        Method to handle remote RDS schema export
        """
        # Get a list of all remote schemas
        self._get_remote_schemas()
        # Format it to be used in expdp
        schemas = "'{0}'".format("','".join(map(str, self.remote_schemas)))
        options = "{0}/{1}@{2} directory=DATA_PUMP_DIR " \
                  "dumpfile={3} logfile={4} REUSE_DUMPFILES=Y " \
                  "schemas={5}".format(self.remote_user, self.remote_password, self.remote_db,
                                       CVDbaas.RDS_EXPORT_SCHEMAFILE,
                                       CVDbaas.RDS_EXPORT_SCHEMALOGFILE, schemas)
        try:
            logging.info('Starting export with options: {0}'.format(options))
            op = self.exportData(options)
            logging.info("Export data output: {0}".format(op))
            if op['return_code'] != 0:
                logging.warning('Export did not complete successfully. Raising error.')
                raise cvException('Export schema failed with error.')
        except Exception:
            raise

    def _create_db_directory(self, location):
        """

        Args:
            location (str): string representing local staging location
        """
        op = self.execSQL("create or replace directory {0} as '{1}'".format(
                CVDbaas.RDS_EXPORT_DB_DIRECTORY, location), True)
        if len(op) == 0:
            logging.info('Local DB Directory {0} created successfully'.format(
                    CVDbaas.RDS_EXPORT_DB_DIRECTORY))

    def _create_db_link(self):
        """
        Method to create local Database Link pointing to the remote RDS database
        """
        op = self.execSQL("create database link {0} connect to {1} identified by {2} using '{3}'".format(
                CVDbaas.RDS_EXPORT_DB_LINK, self.remote_user, self.remote_password,
                self.remote_db), True)
        if len(op) == 0:
            logging.info('Local DB Link {0} created successfully'.format(
                    CVDbaas.RDS_EXPORT_DB_LINK))

    def _transfer_remote_dump(self, location):
        """
        Method to transfer expdp dumpfile from remote RDS directory to local directory
        """
        sql = "EXEC DBMS_FILE_TRANSFER.get_file(source_directory_object => 'DATA_PUMP_DIR', " \
              "source_file_name => '{0}', source_database => '{1}', " \
              "destination_directory_object => '{2}',  " \
              "destination_file_name =>  '{3}')".format(CVDbaas.RDS_EXPORT_SCHEMAFILE,
                                                          CVDbaas.RDS_EXPORT_DB_LINK,
                                     CVDbaas.RDS_EXPORT_DB_DIRECTORY, CVDbaas.RDS_EXPORT_SCHEMAFILE)
        logging.info('Running transfer file sql: {0}'.format(sql))
        # sql_file = os.path.join(location, 'transfer_expdp.sql')
        # with open(sql_file, 'w') as fd:
        #     fd.write(sql)
        # op = self.execsql('@{0}'.format(sql_file), True)
        op = self.execSQL(sql, True)
        if len(op) == 0:
            logging.info('Transfer of expdp dumpfile {0} copied successfully'.format(
                    CVDbaas.RDS_EXPORT_SCHEMAFILE))

    def _import_schemas(self):
        """
        Method to import schemas into the local database
        """
        schemas = "'{0}'".format("','".join(map(str, self.remote_schemas)))
        options = "'/ as sysdba' directory={0}  dumpfile={1} logfile=imp_{2} " \
                  "schemas={3}".format(CVDbaas.RDS_EXPORT_DB_DIRECTORY,
                                       CVDbaas.RDS_EXPORT_SCHEMAFILE,
                                       CVDbaas.RDS_EXPORT_SCHEMALOGFILE, schemas)
        try:
            logging.info('Starting import with options: {0}'.format(options))
            op = self.importData(options)
            logging.info("Import data output: {0}".format(op))
            if op['return_code'] != 0:
                logging.warning('Import did not complete successfully. Raising error.')
                raise cvException('Import schema failed with error.')
        except Exception:
            raise

    def remote_export(self, local_directory):
        # Method to export remote RDS data to the local machine
        """

        Args:
            local_directory (str): Location of the local staging directory
        """
        self._remote_schema_export()
        # -	Create DB directory object
        self._create_db_directory(local_directory)
        # -	Create DB link to RDS
        self._create_db_link()
        # -	Transfer files from RDS
        self._transfer_remote_dump(local_directory)
        # Tablespace in target -- assumes that user has created necessary tablespaces and schemas
        # -	Import schema
        self._import_schemas()

if __name__ == "__main__":
    print('Invalid usage.')
