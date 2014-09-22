# gseCreateViewsPostgreSQL.py - Create views from ChangeDetection nodes in Playlist
# ---------------------------------------------------------------------------
# Created on: 20140115 SG
#
# Description: Get values from change detection nodes in xml files and construct view statements
# ---------------------------------------------------------------------------

import os, sys, arcpy, datetime, xml.dom.minidom

dbSchema = 'sde'
topFolderName = "gse"

ospath = os.path.realpath(__file__)
print ospath
cstr = "gse"
gsepath = ospath[:ospath.rfind(cstr)+len(cstr)]

gseFolder = gsepath
gzFolder = gsepath[:gsepath.rfind(os.sep)] + "\\Tools\\arcpy" # gz tools must be parallel to the gse folder
fmeFolder = gsepath + "\\ETL\\fme\\"
configFolder = gsepath + "\\ETL\\config\\"
etlFolder = gsepath + "\\ETL\\"
sdeConnFolder = gsepath + "\\ETL\\serverConfig"
pyFolder = gsepath + "\\ETL\\py\\"

if gzFolder not in sys.path:
     sys.path.insert(0, gzFolder)
import gzSupport

if pyFolder not in sys.path:
    sys.path.insert(0, pyFolder)


# Script arguments
playlist_xml = arcpy.GetParameterAsText(0) # one playlist xml value.
if playlist_xml == '#' or playlist_xml == None or playlist_xml == '':
    playlist_xml = "gsePlaylistAll.xml" # default value

productionWSName = arcpy.GetParameterAsText(1) # Database name for production Workspace
if productionWSName == '#' or productionWSName == None or productionWSName == '':
    productionWSName = "UWGISProduction"

server = arcpy.GetParameterAsText(2) # server name for production workspace
if server == '#' or server == None or server == '':
    server = "127.0.0.1"

port = arcpy.GetParameterAsText(3) # server port for production workspace
if port == '#' or port == None or port == '':
    port = "5432"

dbname = arcpy.GetParameterAsText(3) # dbname name for production workspace
if dbname == '#' or dbname == None or dbname == '':
    dbname = "vtgisproduction"

user = arcpy.GetParameterAsText(4) # server name for production workspace
if user == '#' or user == None or user == '':
    user = "sde"

password = arcpy.GetParameterAsText(5) # server name for production workspace
if password == '#' or password == None or password == '':
    password = "sde"

sde = arcpy.GetParameterAsText(6) # sde Connection file for views Workspace
if sde == '#' or sde == None or sde == '':
    sde = os.path.join(sdeConnFolder,"GIS Staging.sde")

log = open(os.path.join(sys.path[0],"Create Views.sql"),"w")

recreate = arcpy.GetParameterAsText(7)
if recreate == '#' or recreate == None or recreate == '':
    recreate = False
elif recreate.lower() == "true":
    recreate = True
else:
    recreate = False


def main(argv = None):
    # process one or more views
    global log, playlists_xml
    outputSuccess = True # default value, will be set to False if any processing errors returned
    processed = 0
    plist = fixConfigPath(playlist_xml)
    xmlDataDoc = xml.dom.minidom.parse(plist)
    datasets = gzSupport.getXmlElements(plist,"Dataset")
    for dataset in datasets:
        name = dataset.getAttributeNode("name").nodeValue
        try:
            name = dataset.getAttributeNode("targetName").nodeValue # special case to handle multi sources to one target.
        except:
            pass
        
        printmsg(name)
        changeNodes = dataset.getElementsByTagName("ChangeDetection")
        if len(changeNodes) == 0:
            printmsg("No Change Detection Node")
        else:
            changeNode = changeNodes[0]
            exceptProd = changeNode.getAttributeNode("exceptProductionView").nodeValue
            exceptStaging = changeNode.getAttributeNode("exceptStagingView").nodeValue
            fieldStr = changeNode.getAttributeNode("viewFields").nodeValue
            fieldStr = fieldStr.replace(" ","")
            fields = fieldStr.split(",")
            eprodSql = getExceptProdViewSql(name,exceptProd,exceptStaging,fields)
            estagingSql = getExceptStagingViewSql(name,exceptProd,exceptStaging,fields)
            retVal = createView(exceptProd,eprodSql)
            if retVal == False:
                outputSuccess = False
            createView(exceptStaging,estagingSql)
            if retVal == False:
                outputSuccess = False

    log.close()

def createView(viewName,sql):
    view = os.path.join(sde,viewName)
    printmsg(view)
    #sql = sql[sql.find("AS SELECT ")+3:]
    retVal = False
    if arcpy.Exists(view) and recreate == False:
        printmsg("View already exists " + viewName)
    else:
        try:
            arcpy.CreateDatabaseView_management(sde,viewName,sql)
            retVal = True
            printmsg("-- View created" + view)
        except:
            printmsg("Failed to Create View")
            gzSupport.showTraceback()
            printmsg(sql)
            retVal = False
    return retVal

def getExceptProdViewSql(dsname,exceptProd,exceptStaging,fields):
    evwname = dsname + "_evw"
    viewSql = ""
    #viewSql = exceptProd + " AS "
    viewSql += " SELECT " + getFieldSql(evwname,fields)
    viewSql += " FROM " + dbSchema + "." + evwname
    viewSql += " EXCEPT "
    viewSql += 'SELECT * FROM   dblink(\'hostaddr='+ server + ' port='+ port +' dbname=' +dbname+' user='+user+' password='+password
    viewSql += '\',\'SELECT ' + getFieldSql(evwname,fields) +' FROM ' + dbSchema + '.' + evwname + '\')' 
    viewSql += ' AS ' + dsname + 'ExceptProduction (' + getFieldDefs(evwname,fields) + ')';
    #printmsg(viewSql)
    return viewSql

def getExceptStagingViewSql(dsname,exceptProd,exceptStaging,fields):
    evwname = dsname + "_evw"
    viewSql = ""
    #viewSql =  exceptStaging + " AS "
    viewSql += 'SELECT * FROM   dblink(\'hostaddr='+ server + ' port='+ port +' dbname=' +dbname+' user='+user+' password='+password
    viewSql += '\',\'SELECT ' + getFieldSql(evwname,fields) +' FROM ' + dbSchema + '.' + evwname + '\')' 
    viewSql += ' AS ' + dsname + 'ExceptStaging (' + getFieldDefs(evwname,fields) + ')';
    viewSql += " EXCEPT "
    viewSql += "SELECT " + getFieldSql(evwname,fields)
    viewSql += " FROM " + dbSchema + "." + evwname
    #printmsg(viewSql)
    return viewSql

##
##    evwname = dsname + "_EVW"
##    viewSql = ""
##    viewSql = "CREATE VIEW " + exceptStaging + " AS SELECT "
##    viewSql += getFieldSql(dsname,fields)
##    viewSql += " FROM " + productionWSName + "." + dbSchema + "." + evwname + " EXCEPT "
##    viewSql += " SELECT " + getFieldSql(dsname,fields)
##    viewSql += " FROM " +  stagingWSName + "." + dbSchema + "." + evwname + ";" + "\nGO"
##
##    msg(viewSql)
##    return viewSql

def getFieldSql(dsname,fields):
    fnum = 0
    fieldSql = ""
    for field in fields:
        if field.upper() == "SHAPE":
            field = "text(ST_AsText(" + dsname + ".shape)) AS shape_text"
            fieldSql += field
        else:
            fieldSql += dsname + "." + field.lower()
        if fnum < len(fields) -1:
            fieldSql += ","
        fnum += 1
    return fieldSql

def getFieldDefs(dsname,fields):
    fnum = 0
    fieldDefSql = ""
    for field in fields:
        if field.upper() == "SHAPE":
            field = 'shape_text'
        fieldDefSql += field.lower() + ' text'
        if fnum < len(fields) -1:
            fieldDefSql += ","
        fnum += 1
    return fieldDefSql


def fixConfigPath(playlist_xml):
    if playlist_xml == None:
        return None
    if not playlist_xml.find(os.sep) > 1:
        playlist_xml = os.path.join(configFolder,playlist_xml)
    return playlist_xml

def printmsg(strVal):
    gzSupport.addMessageLocal(strVal)

def msg(val):
    # simple print message function - want to log messages to screen and log file
    strVal = str(val)
    global log
    log.write(strVal + "\n")


if __name__ == "__main__":
    main()
