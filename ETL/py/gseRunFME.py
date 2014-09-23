# gseRunFME.py - Run FME as a subprocess to load from CAD and GDB to GDB
# SG January 2014
# ---------------------------------------------------------------------------

import os, sys

ospath = os.path.realpath(__file__)
etl = os.sep+'ETL'+os.sep
gsepath = ospath[:ospath.rfind(etl)]
pypath = gsepath + etl + 'py'
if (pypath) not in sys.path:
	sys.path.insert(0, pypath)
	print pypath

import time, subprocess, gse, gseDrawing

runAs = "FME" # default to FME

import gseDrawing

def load(inputDrawing,fmeExe,fmeFile,GISStaging_sde,GISProduction_sde,sourceEPSG,runas,playlist_xml,source,fread,fwrite):

    global runAs
    print fread
    print fwrite
    ret = False
    sTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    logFolder = fmeFile[:fmeFile.rfind(os.sep)] + os.sep + "log" + os.sep
    logFile = logFolder + fmeFile[fmeFile.rfind(os.sep)+1:].replace(".fmw",".log")
    runAs = runas 
    if not sourceEPSG.upper().startswith("EPSG:"):
        sourceEPSG = "EPSG:" + sourceEPSG
    print ("Creating subprocess for: " + fmeFile[fmeFile.rfind(os.sep)+1:] + "\n")
    try:
        if source == "CAD":
            comm = getCADCommString(inputDrawing,fmeExe,fmeFile,GISStaging_sde,GISProduction_sde,sourceEPSG,playlist_xml,logFile,fread,fwrite)
        elif source == "GDB":
            comm = getGDBCommString(inputDrawing,fmeExe,fmeFile,GISStaging_sde,GISProduction_sde,sourceEPSG,playlist_xml,logFile,fread,fwrite)
        else:
            print "No config file available for mode: " + source
            return false
        if runAs == "FME":
            printComm(comm)
            retcode = subprocess.call(comm, shell=True)
        elif runAs == "DataInterop":
            print (runAs + " \'runas\' mode is NOT currently supported")
            raise "runasError"
            # 2 issues: can't run data interop from script, can't import arcpy in this .py file without fme.exe errors...
            #gse = os.path.join(etlFolder,"gse.tbx")
            #if checkLicense() == True:
            #    arcpy.ImportToolbox(gse)
            #    arcpy.RefreshCatalog(gse)
            #    func = getDataInteropMethod(fmeFile)
            #    tools = arcpy.ListTools("*gse*")
            #    for tool in tools:
            #        arcpy.AddMessage(tool)
                #(*comm) # split out string into an argument list
            #    retcode = arcpy.gseFloorplanLoaderFME_gse(comm)
                #retcode = subprocess.call(comm, shell=True)
            #else:
            retcode = False
        if retcode != 0:
            ret = False
            print("FME returned an error, code= "  + str(retcode))
        else:
            ret = True
            print("FME returned: " + fmeFile + " loaded: code=" + str(retcode))
    except:
        print("Error: ")
        print("Completed fme processing with exception raised\n")
        ret = False

    finally:
        print("Completed fme process\n")
        return ret

def getCADCommString(inputDrawing,fmeExe,fmeFile,GISStaging_sde,GISProduction_sde,sourceEPSG,playlist_xml,logFile,fread,fwrite):

    floorID = getFloorID(inputDrawing)
    #worldFile = inputDrawing[:inputDrawing.rfind(os.sep)+1] + "esri_cad.wld"

    line1 = getLine1(fmeFile,fmeExe)
    comm = line1 + \
    makeFMEParam("DestDataset_GEODATABASE_SDE","sde") + \
    makeFMEParam("OUT_CONNECTION_FILE_GEODATABASE_SDE",GISStaging_sde) + \
    makeFMEParam("ProductionConnectionFile",GISProduction_sde) + \
    makeFMEParam("GizintaProject",playlist_xml) + \
    makeFMEParam("TargetFieldQA","True") + \
    makeFMEParam("SourceFieldQA","False") + \
    makeFMEParam("IgnoreErrors","False") + \
    makeFMEParam("SourceDatasetTypes","CADDataset") + \
    makeFMEParam("pFLOORID",floorID) + \
    makeFMEParam("SourceCoordinateSystem",sourceEPSG) + \
    makeFMEParam("gzDebug","False") + \
    makeFMEParam("LOG_FILE",logFile) + \
    makeFMEParam("FEATURE_TYPES",fwrite) + \
    makeFMEParam("FEATURE_TYPES_READ",fread) + \
    makeFMEParam("SourceDataset_ESRIMSD",inputDrawing)


    #makeFMEParam("SCHEMA_IN_REAL_FORMAT_SCHEMA_MSD","ESRIMSD") + \

    comm = getCloseString(comm)
    #SourceCoordinateSystem
    #makeFMEParam("SourceWorldFile",worldFile)
    return comm

def getGDBCommString(inputDrawing,fmeExe,fmeFile,GISStaging_sde,GISProduction_sde,sourceEPSG,playlist_xml,logFile,fread,fwrite):

    #dwg = inputDrawing[inputDrawing.rfind(os.sep)+1:]
    floorID = getFloorID(inputDrawing)

    line1 = getLine1(fmeFile,fmeExe)

    comm =  line1 + \
    makeFMEParam("DestDataset_GEODATABASE_SDE","sde") + \
    makeFMEParam("SCHEMA_IN_REAL_FORMAT_SCHEMA","Esri Geodatabase (ArcSDE Geodatabase)") + \
    makeFMEParam("OUT_CONNECTION_FILE_GEODATABASE_SDE",GISStaging_sde) + \
    makeFMEParam("ProductionConnectionFile",GISProduction_sde) + \
    makeFMEParam("GizintaProject",playlist_xml) + \
    makeFMEParam("SourceFieldQA","False") + \
    makeFMEParam("TargetFieldQA","True") + \
    makeFMEParam("IgnoreErrors","False") + \
    makeFMEParam("SourceDatasetTypes","GDBDataset") + \
    makeFMEParam("pFLOORID",floorID) + \
    makeFMEParam("SourceCoordinateSystem",sourceEPSG) + \
    makeFMEParam("gzDebug","False") + \
    makeFMEParam("FEATURE_TYPES",fwrite) + \
    makeFMEParam("FEATURE_TYPES_READ",fread) + \
    makeFMEParam("LOG_FILE",logFile)

    comm = getCloseString(comm)

    return comm

def makeFMEParam(pName, pValue):
    # prepare FME parameters for sending to the FME subprocess as command line arguments
    global runAs
    if runAs == "FME":
        param = " --" + pName + " \"" + pValue + "\""
        
        return param
    elif runAs == "DataInterop":
#        if pValue.lower() in ['true','false']:
#            param = pValue + ","
#        else:
        if pValue.find(os.sep) > -1:
            param = "r\'" + pValue + "\',"
        else:
            param = "\'" + pValue + "\',"
                
        return param


def getLine1(fmeFile,fmeExe):
    global runAs
    if runAs == "FME":
        line1 = "\"" + fmeExe + "\"" + " \"" + fmeFile + "\""
    elif runAs == "DataInterop":
        line1 = ""
    return line1

def getCloseString(comm):
    global runAs
    if runAs == "FME":
        closeStr = ""
    elif runAs == "DataInterop":
        closeStr = ""
    comm = comm + closeStr
    if comm.endswith(","):
        comm = comm[:len(comm)-1]
    return comm

def getFloorID(inputDrawing):
    floorID = ""
    try:
        floorID = gseDrawing.getFloorIDFromPath(inputDrawing) 
    except:
        floorID = ""
    return floorID

def printComm(comm):
    parts = comm.split('--')
    p =0
    for part in parts:
        print ("--" if p > 0 else "") + part
        p += 1
    

##def checkLicense():
##    class LicenseError(Exception):
##        pass
##    try:
##        if arcpy.CheckExtension("DataInteroperability") == "Available":
##            print arcpy.CheckOutExtension("DataInteroperability")
##            print "Checked out \"DataInteroperability\" Extension"
##            return True
##        if arcpy.CheckExtension("FME") == "Available":
##            arcpy.CheckOutExtension("FME")
##            print "Checked out \"FME\" Extension"
##            return True
##        else:
##            raise LicenseError
##    except LicenseError:
##        print "Data Interoperability license is unavailable"
##    except:
##        print arcpy.GetMessages(2)
##
##    return False

##def getDataInteropMethod(fmeFile):
##
##    funcName = None
##
##    if fmeFile != None:
##        try:
##            fmeName = fmeFile[fmeFile.rfind(os.sep)+1:].replace(".fmw","")
##            funcName = getattr(arcpy, fmeName +"_gse")
##        except:
##            print "Error finding function for " + fmeFile
##    return funcName    