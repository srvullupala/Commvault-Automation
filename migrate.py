#!/usr/bin/env python
#############################################################################################################
# Name          : main.py
# Created By    : Anirudh S Narayan, Shankar Vullupala
#
# Description   : Main script used to call oracle migration
# Notes         :
# Exit Codes    : 1-5   Export phase related
#                 6-10  Transfer phase related
#                 11-15 Import phase related
#                 16-20 Cleanup phase related
#############################################################################################################
try:
    import cvhelper
    from getpass import getpass
    from cvepmigration import *
except ImportError:
    print('Unable to import cvhelper and cvepmigration. Exiting')
    raise

try:
    import argparse
except ImportError:
    logging.info('Unable to import argparse module. Exiting.')
    raise


def parseArgs(arg):
    """TODO: Docstring for parseArgs.
    :returns: TODO

    """
    parser=argparse.ArgumentParser(description='Commvault Oracle Migration Utility')
    parser.add_argument('-m','--mode',nargs=1,choices=['rman_convert','ts','full','sch','crosscloud','imageCopy'],
                        help='Mode of migration.')
    parser.add_argument('-ru','--remote_user',nargs=1,help='Remote oracle user name')
    parser.add_argument('-rp','--remote_passwd',nargs=1,help='Remote oracle user password')
    parser.add_argument('-rd','--remote_db',nargs=1,help='Remote oracle database name')
    parser.add_argument('-fl','--filelist',nargs=1,help='File list to read during import mode')
    parser.add_argument('-imcl','--imglocation',nargs=1,help='image copy location')
    rparser=parser.add_argument_group('required named argument')
    rparser.add_argument('-em','--exportmode',required=False,nargs=1,choices=['export','import','complete','crosscloud'],
                         help='Stop/start script at what point. Defaults to complete.')
    rparser.add_argument('-d','--database',required=False,nargs=1,help='Source database name')
    rparser.add_argument('-l','--location',required=True,nargs=1,help='OS Directory location for storing files')
    rparser.add_argument('-e','--entries',nargs='+',help='Space Separated List of source values')
    args=parser.parse_args()
    #-------'RMAN Conversion of image copy datafiles in target OS Begins'----------
    if (args.imglocation != None and args.location != None):
        #args.mode[0] == 'imageCopy'
        print("******RMAN Convert and Import operation ******")
        logging.info("******RMAN Convert and Import operation ******")
        logging.info('Arguments parsed from command line: '+str(args))
        cv = CvOraExport(None)
        if args.remote_user is None or args.remote_passwd is None or args.remote_db is None:
            cvhelper.info("Taken inputs from user")
            user = raw_input('Enter Oracle connect user[/]:  ')
            if user is '':
                cv.setRemoteCreds(None, None, None)
            else:
                pwd = getpass("Oracle connect password: ")
                cv.setRemoteCreds(user, pwd, None)
        else:
            cv.setRemoteCreds(args.remote_user[0], args.remote_passwd[0], args.remote_db[0])
        cv.setImageCopyMode(imagecopy_path=args.imglocation[0], dest_path=args.location[0])
        try:
            if cv.preChecks():
                if cv.createDDLImgCpy():
                    if cv.rmanConvertImgCpy():
                        print("****** RMAN Convert and Import operation complete ******")
                        logging.info("RMAN Convert and Import operation complete")
                        return
        except (ValueError, cvException, Exception) as arg:
            logging.error(str(arg))
            logging.warning('Errors detected.')
            print ('ERROR: Errors detected - '+str(arg))
            sys.exit(18)
        except:
            logging.warning('Unrecognized error.')
            print ('ERROR: Unexpected errors detected')
            sys.exit(19)
    else:
        args=parser.parse_args()
        #logging.info('Arguments parsed from command line: '+str(args))
        #if args.mode[0] == 'imageCopy':
            #logging.info('Stopping script')
            #return
        if args.mode[0] == 'crosscloud':
            cv = CVDbaas(args.database[0])
            cv.set_remote_credentials(user=args.remote_user[0], passwd = args.remote_passwd[0],
                                      db = args.remote_db[0])
            cv.remote_export(args.location[0])
            sys.exit(0)
        cv = CvOraExport(args.database[0])
        cv.setTermMode(args.exportmode[0])
        if args.exportmode[0]=='complete':
            if args.remote_user is None or args.remote_passwd is None or args.remote_db is None:
                raise ValueError('Remote machine details not provided.')
            cv.setRemoteCreds(args.remote_user[0], args.remote_passwd[0], args.remote_db[0])
        if args.exportmode[0]=='import':
            if args.filelist is None:
                raise ValueError('File list to import from not provided.')
            cv.setFileList(args.filelist[0])
        if args.mode[0]=='sch':
            cv.setExportMode(mode='schemas', location=args.location[0], entries=args.entries)
            cvhelper.info("Schema List: " + str(args.entries))
        elif args.mode[0]=='full':
            cv.setExportMode(mode='full', location=args.location[0], entries=args.entries)
            cvhelper.info("Full DB entries: " + str(args.entries))
        elif args.mode[0]=='ts':
            cv.setExportMode(mode='tablespaces', location=args.location[0], entries=args.entries)
            cvhelper.info("TS List: " + str(args.entries))
        elif args.mode[0]=='rman_convert':
            cv.setExportMode(mode='transport_tablespaces', location=args.location[0], entries=args.entries)
            cvhelper.info("Transportable TS List: "+str(args.entries))
        else:
            cvhelper.error('Invalid export mode. Exiting')
            print ('ERROR: Unrecognized export mode')
            return
        if args.exportmode[0]=='export' or args.exportmode[0]=='complete':
            try:
                cv.validationPhase()
                cv.exportPhase()
                cv.prepareMeta()
                if args.exportmode[0]=='export':
                    logging.info('Stopping script after export phase.')
                    return
            except (ValueError, cvException, Exception) as arg :
                logging.error(str(arg))
                logging.info('Exiting with errors from export phase. Please check the logs.'+str(arg))
                print ('ERROR: Errors detectd in export phase - '+str(arg))
                sys.exit(1)
            except:
                logging.error('Unrecognized error in export phase')
                print ('ERROR: Unexpected errors detectd in export phase')
                sys.exit(2)
            try:
                cv.transferPhase()
            except (ValueError, cvException, Exception) as arg :
                logging.error(str(arg))
                logging.info('Exiting with errors from transfer phase. Please check the logs.'+str(arg))
                print ('ERROR: Errors detected at transfer phase - '+str(arg))
                sys.exit(6)
            except:
                logging.error('Unrecognized error in transfer phase'+str(arg))
                print ('ERROR: Unexpected error at transfer')
                sys.exit(7)
        try:
            if args.entries[0]=='full':
                cv.importPhase(True)
            else:
                cv.importPhase()
        except (ValueError, cvException, Exception) as arg :
            logging.error(str(arg))
            logging.info('Exiting with errors at import phase. Please check the logs.')
            print ('WARNING: Errots detected at Import - '+str(arg))
            sys.exit (arg.return_code)
            # sys.exit(11)
        except:
            logging.error('Unrecognized error in import phase.')
            print ('ERROR: Unexpected error at import')
            sys.exit(12)
        if args.exportmode[0]=='complete':
            try:
                cv.cleanupPhase()
            except (ValueError, cvException, Exception) as arg :
                logging.error(str(arg))
                logging.warning('Errors detected at cleanup phase. Please check the logs and rectify it.')
                print ('ERROR: Errors detectd in cleanup phase - '+str(arg))
                sys.exit(16)
            except:
                logging.warning('Unrecognized error in cleanup phase.')
                print ('ERROR: Unexpected errors detectd in cleanup phase')
                sys.exit(17)





if __name__=="__main__":
    cvhelper.setLogging("/tmp/migrateOracle.log")
    cvhelper.info("##### Application Starts #######")
    parseArgs(sys.argv[1:])
    cvhelper.info("##### Application Ends #######")
