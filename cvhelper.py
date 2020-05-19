#!/usr/bin/env python
#############################################################################################################
# Name          : cvhelper.py
# Created By    : Anirudh S Narayan, Shankar Vullupala
#
# Description   : A helper script to assist in quick oracle-commvault development
# Notes         :
#############################################################################################################
try:
    import platform
    import sys
    import os
except ImportError:
    print('Unable to import system modules. Exiting.')
    raise
try:
    import re
    import subprocess
except ImportError:
    print('Unable to regex and subprocess modules. Exiting.')
    raise
try:
    import logging
    from logging.config import dictConfig
except ImportError:
    print('Unable to logging modules. Exiting.')
    raise

def setLogging(_fn="cvOraHelper.log"):
    logging_config = dict(
    version = 1,
    formatters = {
        'f': {'format':'%(asctime)s %(name)-3s: %(levelname)-2s : %(message)s',
            'datefmt': '%m-%d-%Y %I:%M:%S %p'
            }
        },
    handlers = {
        'default' : {'level': logging.DEBUG,
            'formatter': 'f',
            'class': 'logging.StreamHandler',},
        'rotate_file': {
            'level': logging.DEBUG,
            'formatter': 'f',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': _fn,
            'maxBytes': 100000,
            'backupCount': 1,}
        },
    root = {
        'handlers': ['rotate_file'],
        'level': logging.DEBUG,
        },
    )
    dictConfig(logging_config)
    logger = logging.getLogger()


def info(_str):
    logging.info(_str)

def debug(_str):
    logging.debug(_str)

def warning(_str):
    logging.warning(_str)

def error(_str):
    logging.error(_str)

def flatten(possiblyNestedList):
    # Flatten abritrarily nested list
    if not isinstance(possiblyNestedList, list):
        return
    flattened = []
    for item in possiblyNestedList:
        if isinstance(item, list):
            flattened.extend(flatten(item))
        else:
            flattened.append(item)
    return flattened


class cvException(Exception):
    def __init__(self, arg, ret_code=0):
        Exception.__init__(self, 'Commvault custom Exception: '+str(arg))
        self.msg = arg
        if ret_code!=0:
            self.return_code=ret_code



class Cvhelper(object):

    """Docstring for cvhelper. """

    def __init__(self, db):
        """TODO: to be defined1. """
        self.cv_oraenv={}
        try:
            self.setEnv()
        except:
            raise ValueError('Unable to set OS Environment.')
        try:
            #self.parseOratab(db)
            self.setOraEnv()
        except:
            raise


    def setEnv(self):
        if platform.system()=='Linux' or platform.system()=='AIX':
            self.cv_oraenv['oratab']='/etc/oratab'
        if platform.system()=='SunOS':
            self.cv_oraenv['oratab']='/var/opt/oracle/oratab'



    def getEnv(self):
        """TODO: Docstring for getEnv.
        :returns: Dictionary of envt variables

        """
        return self.cv_oraenv


    def parseOratab(self, db):
        """TODO: Docstring for parseOratab.
        :returns: Void

        """
        f=open(self.cv_oraenv['oratab'], 'r')
        for line in f:
            if re.search('^$', line):
                continue
            if line.startswith('#'):
                continue
            (sdb, sdbh, sautostart)=line.split(':')
            if db == sdb:
                self.cv_oraenv['sid']           =db
                self.cv_oraenv['oracle_home']   =sdbh
                return
        f.close()
        raise ValueError('Unable to set Oracle Environment.')

    def setOraEnv(self):
        """TODO: Docstring for setOraEnv.
        :returns: Void

        """
        try:
            self.cv_oraenv["sid"] = subprocess.check_output('echo $ORACLE_SID', shell=True).strip( )
            self.cv_oraenv["oracle_home"] = subprocess.check_output('echo $ORACLE_HOME', shell=True).strip( )
            logging.info('ORACLE_SID in oraenv: ' + str(self.cv_oraenv['sid']))
            pmt = raw_input('Enter ORACLE_SID of target database <' + str(self.cv_oraenv['sid']) + '>: ')
            if pmt is '' or pmt is ' ':
                logging.info('ORACLE_SID is set to ' + str(self.cv_oraenv['sid']))
            else:
                self.cv_oraenv['sid'] = pmt
                logging.info('ORACLE_SID is set to ' + str(self.cv_oraenv['sid']))
            logging.info('ORACLE_HOME in oraenv: ' + str(self.cv_oraenv['oracle_home']))
            pmt = raw_input('Enter ORACLE_HOME of target database <' + str(self.cv_oraenv['oracle_home']) + '>: ')
            if pmt is '' or pmt is ' ':
                logging.info('ORACLE_HOME is set to ' + str(self.cv_oraenv['oracle_home']))
            else:
                self.cv_oraenv['oracle_home'] = pmt
                logging.info('ORACLE_HOME is set to ' + str(self.cv_oraenv['oracle_home']))
            return
        except:
            raise ValueError('Unable to set Oracle Environment.')

    def getOraEnv(self):
        """TODO: Docstring for sgetEnv.
        :returns: TODO

        """
        env = os.environ
        env["ORACLE_HOME"] = self.cv_oraenv["oracle_home"]
        env["ORACLE_SID"] = self.cv_oraenv["sid"]
        env["LD_LIBRARY_PATH"] = os.path.join(self.cv_oraenv["oracle_home"], 'lib')
        return env

    def execSQL(self, stmt, sys=False, **par):
        """TODO: Docstring for execSQL.
        :returns: output of SQLPlus command

        """
        sqlp = os.path.join(self.cv_oraenv["oracle_home"], 'bin', "sqlplus")
        cmd = [sqlp]
        if sys:
            cmd.append('/ as sysdba')
        else:
            x = ''
            if 'user' in par and 'password' in par:
                x = par['user'] + '/' + par['password']
            else:
                raise ValueError('User and Password not provided')
            if 'db' in par:
                x += '@' + par['db']
            cmd.append(x)
        try:
            p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,env=self.getOraEnv())
            (out, err) = p.communicate('set head off ver off lines 200 pages 0 feed off colsep |\n' + stmt + ';\nexit\n')
            ret = p.returncode
            if ret != 0:
                raise cvException("Error logging into database. SQLPlus exited with return code " + str(ret))
            lines = ''
            if out:
                lines = out.strip().split('\n')
            if err:
                lines = err.strip().split()
            return [[col.strip() for col in line.split('|')] for line in lines]
        except cvException as e:
            raise
        except:
            raise ValueError('Error executing SQL for ' + stmt)


    def rmanExec(self, options):
        """TODO: Docstring for rmanExec.
        :returns: TODO

        """
        exp= os.path.join(self.cv_oraenv["oracle_home"], 'bin', "rman")
        cmd=[exp, options]
        try:
            p           = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.getOraEnv())
            (out, err)  = p.communicate()
            ret     = p.returncode
            if out:
                lines   = out.strip().split('\n')
            if err:
                lines   = err.strip().split('\n')
            return {'return_code': ret, 'output': lines}
        except:
            raise ValueError('Error running RMAN script for '+options)


    def exportData(self, options):
        """TODO: Docstring for execSQL.
        :returns: output of expdp command

        """
        exp= os.path.join(self.cv_oraenv["oracle_home"], 'bin', "expdp")
        cmd=[exp, options]
        try:
            p           = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.getOraEnv())
            (out, err)  = p.communicate()
            ret     = p.returncode
            if out:
                lines   = out.strip().split('\n')
            if err:
                lines   = err.strip().split('\n')
            return {'return_code': ret, 'output': lines}
        except:
            raise ValueError('Error running export for '+options)



    def importData(self, options):
        """TODO: Docstring for execSQL.
        :returns: output of expdp command

        """
        imp= os.path.join(self.cv_oraenv["oracle_home"], 'bin', "impdp")
        cmd=[imp, options]
        try:
            p           = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.getOraEnv())
            (out, err)  = p.communicate()
            ret     = p.returncode
            if out:
                lines   = out.strip().split('\n')
            if err:
                lines   = err.strip().split('\n')
            return {'return_code': ret, 'output': lines}
        except:
            raise ValueError('Error running import for '+options)



if __name__=="__main__":
    print("Invalid Usage.")
