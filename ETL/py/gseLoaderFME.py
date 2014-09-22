# gseLoaderFME.py - load Campus drawings using FME and arcpy
# ---------------------------------------------------------------------------
# Created on: 2013-12-19 SG
#
# Description: load floorplans for a campus to staging database using Gizinta FME approach plus custom sync to production
# ---------------------------------------------------------------------------

# Imports
import os, sys, arcpy, datetime, xml.dom.minidom

topFolderName = "gse"

# Script arguments
playlists_xml = arcpy.GetParameterAsText(0) # one or more playlist xml values, separated by commas.
if playlists_xml == '#' or playlists_xml == None or playlists_xml == '':
#    playlists_xml = "fpbldgTiltLoadPlaylist.xml" # default value
#    playlists_xml = "fpLoadPlaylist.xml,fpDerivePlaylist.xml" # default value
    playlists_xml = "fpLoadPlaylist.xml,fpDerivePlaylist.xml" # default value
gseData_xml = arcpy.GetParameterAsText(1) # settings for CAD folder and GDB
if gseData_xml == '#' or gseData_xml == None or gseData_xml == '':
#    gseData_xml = "gseDataConfig.xml" # default value
    gseData_xml = "gseDataConfig.xml" # default value
autoSync = arcpy.GetParameterAsText(2)
if autoSync == '#' or autoSync == None or autoSync == '':
    autoSync = True
elif autoSync.lower() == "true":
    autoSync = True
else:
    autoSync = False

exitOnError = arcpy.GetParameterAsText(3)
if exitOnError == '#' or exitOnError == None or exitOnError == '':
    exitOnError = True
elif exitOnError.lower() == "true":
    exitOnError = True
else:
    exitOnError = False


successParam = 4

print "Playlist Parameters '" + playlists_xml + "'"
import gseRunFME # have to load after params fetched
import gzSupport

log = None

def main(argv = None):
    # process one or more drawings
    global log, playlists_xml
    outputSuccess = True # default value, will be set to False if any processing errors returned
    doImports()
    processed = 0
    errorCount = 0
    cfgfile = fixServerConfigPath(gseData_xml)
    xmlDataDoc = xml.dom.minidom.parse(cfgfile)
    gseData = gseDataSettings(xmlDataDoc)
    playlists_xml = playlists_xml.split(",")
    gses = []
    playlists = []
    for playlist in playlists_xml:
        filepath = fixConfigPath(playlist)
        playlists.append(filepath)
        xmlDoc = xml.dom.minidom.parse(filepath)
        gse = gseSettings(xmlDoc,gseData)
        gses.append(gse)
    log = open(gses[0].logFileName,"w")
    try:
        totalTime = gzSupport.timer(0)
        inputFiles = gzSupport.getFileList(gses[0].cadFolder,gses[0].fileExt,gses[0].minTime)

        for fileFound in inputFiles:
            if errorCount > 0 and exitOnError == True:
                break
            folder = fileFound[0]
            dwg = fileFound[1]
            drawingTime = gzSupport.timer(0)
            pVal = 0 # counter for playlist looping
            partFailed = False
            if(dwg.find(gses[pVal].nameContains) > -1):
                msg("\n" + dwg)
                for playlist in playlists: # Loop through the playlists and do the loading from CAD
                    if cont(errorCount,exitOnError,partFailed): # stop processing if any errors or continue if exit on error param is false
                            retVal = doLoad(playlist,folder,dwg,gses[pVal]) # Load the playlist using FME subprocess
                            if(retVal != True):
                                outputSuccess = False
                                errorCount += 1
                                gses[pVal].loaded = False
                                partFailed = True
                            else:
                                gses[pVal].loaded = True
                    pVal += 1
                if (errorCount,exitOnError,partFailed):
                    pVal = 0
                    for playlist in playlists: # go back through the playlists and Sync for this drawing
                        if errorCount == 0 and autoSync == True: # Sync is param set and no errors have been returned
                            retVal = doSync(playlist,folder,dwg,gses[pVal]) # sync from Staging to Production
                            if(retVal != True):
                                outputSuccess = False
                                errorCount += 1
                                gses[pVal].syncd = False
                            else:
                                gses[pVal].syncd = True
                        pVal += 1
                loaded = False
                for gse in gses:
                    if (gse.loaded == True or gse.syncd == True) and dwg.find(gse.nameContains) > -1: # if any load or sync processing happened...
                        loaded = True
                if loaded:
                    msg(dwg + " total processing time: " + getTimeElapsed(drawingTime))
                    msg("Number of Errors = " + str(errorCount))
                    processed += 1
                    if gses[0].deleteCADFiles == True:
                        try:
                            os.remove(os.path.join(folder,dwg))
                        except:
                            msg("Unable to delete CAD file " + dwg + "... continuing")
                gzSupport.cleanupGarbage()
    except:
        errorCount += 1
        msg("A fatal error was encountered in gseLoaderFME.py")
        gzSupport.showTraceback()
        outputSuccess = False
        logProcess("gseLoaderFME","drawings",outputSuccess,gses[0].stagingWS)

    finally:
        arcpy.SetParameterAsText(successParam,outputSuccess)
        msg("Total Number of Errors = " + str(errorCount))
        msg("outputSuccess set to: " + str(outputSuccess) + ", " + str(processed) + " drawings processed")
        msg("Total Processing time: " + getTimeElapsed(totalTime) + "\n")
        del gses, playlists
        log.close()

def cont(errorCount,exitOnError,partFailed):
    contin = False
    if ((errorCount == 0 and exitOnError == True) or exitOnError == False) and partFailed == False:
        contin = True
    return contin

def doLoad(playlist_xml,folder,dwg,gse):
    # Load process drawing
    global log
    outputSuccess = False # default value

    inputDrawing = os.path.join(folder,dwg)
    drawingTime = gzSupport.timer(0)
    msg("\nLoading " + dwg + " to database")
    msg("fmeFile = " + gse.fmeLoadFile[gse.fmeLoadFile.rfind(os.sep)+1:])
    # load using FME
    if gse.fmeLoadFile == None or gse.fmeLoadFile == "" or gse.fmeLoadFile == fmeFolder:
        msg("No FME file for loading")
        retVal = True
    else:
        retVal = gseRunFME.load(inputDrawing,gse.fmeExe,gse.fmeLoadFile,gse.stagingWS,gse.productionWS,gse.sourceEPSG,gse.runas,playlist_xml,gse.source,
                getFeatureTypes(playlist_xml,"sourceName"),getFeatureTypes(playlist_xml,"targetName"))
        msg("FME processing time: " + getTimeElapsed(drawingTime))
        logProcess(gse.fmeLoadFile[:gse.fmeLoadFile.rfind(os.sep)+1],dwg,retVal,gse.stagingWS)
    msg(dwg + " Load processing time: " + getTimeElapsed(drawingTime) )
    gzSupport.cleanupGarbage()

    msg("return value set to: " + str(retVal))
    return retVal

def getFeatureTypes(playlist,nm):
    ds = gzSupport.getDatasets(playlist)
    vals = []
    for d in ds:
        vals.append(d.getAttributeNode(nm).nodeValue)
    strVals = " ".join(vals)
    return strVals

def doSync(playlist_xml,folder,dwg,gse):
    # Sync process drawing
    global log
    inputDrawing = os.path.join(folder,dwg)
    drawingTime = gzSupport.timer(0)
    msg("Sync changes to database for " + dwg)
    # sync changes
    result = arcpy.gseSyncChanges_gse(inputDrawing,playlist_xml,gse.stagingWS,gse.productionWS)
    if result.getOutput(0) != None and result.getOutput(0).lower() == 'true':
        retVal=True
    else:
        retVal=False
    logProcess("Sync to Production",dwg,retVal,gse.productionWS)
    msg(dwg + " Sync processing time: " + getTimeElapsed(drawingTime) )
    gzSupport.cleanupGarbage()

    msg("return value set to: " + str(retVal))
    return retVal

def getTimeElapsed(timeVal):
    # format elapsed time string
    return (str(int(gzSupport.timer(timeVal)/60)) + 'm' + str(int(gzSupport.timer(timeVal) % 60)) + 's')

def msg(val):
    # simple print message function - want to log messages to screen and log file
    strVal = str(val)
    gzSupport.addMessageLocal(strVal)
    global log
    log.write(strVal + "\n")

def logProcess(name,dwg,retVal,ws):
    gzSupport.workspace = ws
    gzSupport.logDatasetProcess(name,dwg,retVal)

ospath = sys.path[0]
print ospath
cstr = topFolderName
gsepath = ospath[:ospath.rfind(cstr)+len(cstr)]

gseFolder = gsepath
gzFolder = gsepath[:gsepath.rfind(os.sep)] + "\\Tools\\arcpy" # gz tools must be parallel to the gse folder
fmeFolder = gsepath + "\\ETL\\fme\\"
configFolder = gsepath + "\\ETL\\config\\"
serverConfigFolder = gsepath + "\\ETL\\serverConfig\\"
etlFolder = gsepath + "\\ETL\\"
sdeConnFolder = serverConfigFolder
pyFolder = gsepath + "\\ETL\\py\\"

if gzFolder not in sys.path:
     sys.path.insert(0, gzFolder)

if pyFolder not in sys.path:
    sys.path.insert(0, pyFolder)
if etlFolder not in sys.path:
    sys.path.insert(0, etlFolder)

def doImports():
    # Load required toolbox
    #arcpy.ImportToolbox(os.path.join(gzFolder[:gzFolder.rfind(os.sep)],"Gizinta.tbx"))
    arcpy.ImportToolbox(os.path.join(etlFolder,"gse.tbx"))

class gseSettings:
    # a simple class to hold the settings used by the tools, only used/visible inside the main function
    def __init__(self,xmlDoc,gseData):
        self.xmlDoc = xmlDoc
        self.dirName = sys.path[0]
        loadSettings = xmlDoc.getElementsByTagName("LoadSettings")[0]
        # items from gseData config
        self.cadFolder = gseData.cadFolder
        self.stagingWS = gseData.stagingWS
        self.productionWS = gseData.productionWS
        self.minTime = gseData.minTime
        self.deleteCADFiles = gseData.deleteCADFiles
        self.fileExt = gseData.fileExt
        self.fmeExe = gseData.fmeExe
        self.sourceEPSG = gseData.sourceEPSG
        self.runas = gseData.runas
        self.nameContains = loadSettings.getAttributeNode("nameContains").nodeValue
        self.logFileName = os.path.join(pyFolder,loadSettings.getAttributeNode("logFileName").nodeValue)
        fmename = loadSettings.getAttributeNode("fmeLoadFile").nodeValue
        if fmename == "" or fmename == "None":
            self.fmeLoadFile = ""
        else:
            self.fmeLoadFile = os.path.join(fmeFolder,fmename)
        self.source = loadSettings.getAttributeNode("source").nodeValue
        self.loaded = False
        self.syncd = False

class gseDataSettings:
    # a simple class to hold the data settings used by the tools, global setting for script
    def __init__(self,xmlDoc):
        self.xmlDoc = xmlDoc
        dataSettings = xmlDoc.getElementsByTagName("Settings")[0]
        self.cadFolder = eval(dataSettings.getAttributeNode("cadFolder").nodeValue)
        self.stagingWS = os.path.join(sdeConnFolder,dataSettings.getAttributeNode("stagingWS").nodeValue)
        self.productionWS = os.path.join(sdeConnFolder,dataSettings.getAttributeNode("productionWS").nodeValue)
        self.minTime = datetime.datetime.strptime(dataSettings.getAttributeNode("minTime").nodeValue,"%d/%m/%Y %I:%M:%S %p")
        self.deleteCADFiles = gzSupport.strToBool(dataSettings.getAttributeNode("deleteCADFiles").nodeValue)
        self.fileExt = dataSettings.getAttributeNode("fileExt").nodeValue
        self.fmeExe = dataSettings.getAttributeNode("fmeExe").nodeValue
        self.sourceEPSG = dataSettings.getAttributeNode("sourceEPSG").nodeValue
        self.runas = dataSettings.getAttributeNode("runas").nodeValue

def fixConfigPath(playlist_xml):
    if playlist_xml == None:
        return None
    if not playlist_xml.find(os.sep) > 1:
        playlist_xml = os.path.join(configFolder,playlist_xml)
    return playlist_xml

def fixServerConfigPath(cxml):
    if cxml == None:
        return None
    if not cxml.find(os.sep) > 1:
        cxml = os.path.join(serverConfigFolder,cxml)
    return cxml


if __name__ == "__main__":
    main()
