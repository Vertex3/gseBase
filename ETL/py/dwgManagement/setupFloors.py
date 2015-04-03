import os, sys, subprocess, arcpy, traceback

workspace = r'C:\Apps\Gizinta\gseUW\ETL\serverConfig\GIS Production.sde'
dwgfolder = r'C:\Apps\Gizinta\CADVault\Floorplans'

dwgs = []
debug = False
arcpy.env.workspace = workspace

def main(argv = None):

    for root, dirs, files in os.walk(dwgfolder,followlinks=True):
        for dir in dirs:
            for root, dirs, files in os.walk(root,followlinks=True):
                for dwg in files:
                    if dwg.lower().endswith(".dwg") and dwg.lower().find("outline") < 0: # and dwg.upper().find("MG") > -1 : example for processing subset
                        # for each drawing split into parts and insert to table.
                        dwg = dwg.replace(".dwg","")
                        try:
                            dwgs.index(dwg)
                        except:
                            parts = dwg.split("XP-")
                            try:
                                bldg = parts[0]
                                flr = parts[1]
                                pths = root.split(os.sep)
                                site = pths[len(pths)-2].split('-')[0]
                                insertActiveFloor(site,dwg,bldg,flr)
                                dwgs.append(dwg)
                            except:
                                print dwg, "not processed"

    print "processed " + str(len(dwgs)) + " drawings", str(dwgs)


def insertActiveFloor(siteid,dwg,bldg,flr):
    tabActive_Floor = os.path.join(workspace,"Active_Floor")
    retcode = False
    if not arcpy.Exists(tabActive_Floor):
        addMessageLocal("table does not exist, exiting")
        retcode = False
    else:
        insertCursor = arcpy.InsertCursor(tabActive_Floor)
        if debug:
            addMessageLocal("Inserting into " +  tabActive_Floor)

        try:
            insRow = insertCursor.newRow()
            buildingid = siteid + "_" + str(bldg)
            floorid = buildingid + "_" + flr
            
            insRow.setValue("SITEID",siteid)
            insRow.setValue("BUILDINGID",buildingid)
            insRow.setValue("FLOORCODE",flr)
            insRow.setValue("FLOORID",floorid)
            insRow.setValue("SOURCEDWG",dwg)
            #insRow.setValue("HASINTERIORSPACES","Y")
            #insRow.setValue("HASFLOORPLANLINES","Y")
            #insRow.setValue("HASFLOORAREA","Y")
            elev = getElev(flr)
            insRow.setValue("SCHELEV",elev)
            insRow.setValue("GROUNDELEV",elev+36)
            insRow.setValue("SENSITIVITY","None")
            insRow.setValue("BLDGCODE",bldg)
            insertCursor.insertRow(insRow)
            addMessageLocal("Row inserted into Active_Floor table for " + dwg)
            retcode = True
        except:
            print "Error inserting row for",dwg
            showTraceback()
    del insertCursor
    return retcode

def getElev(flr):
    elev = 0
    if flr[0] == 'B':
        elev = - int(flr[1:]) 
    elif flr[0] == 'M':
        if flr[1] == 'G':
            elev = 0.5
        else:
            elev = int(flr[1:]) + 0.5
    elif flr == '0G' or flr == 'MG':
        elev = 0
    elif flr == 'RF':
        elev = 9.8
    else:
        try:
            elev = int(flr)
        except:
            elev = 0
            print "Error getting floor level, defaulted to 0"

    return (elev * 10)


def addMessageLocal(txt):
    print txt
def showTraceback():
    # get the traceback object and print it out
    tBack = sys.exc_info()[2]
    # tbinfo contains the line number that the code failed on and the code from that line
    tbinfo = traceback.format_tb(tBack)
    tbStr = ""
    for i in range(0,len(tbinfo)):
        tbStr = tbStr + str(tbinfo[i])
    # concatenate information together concerning the error into a message string
    pymsg = "Python Error messages:\nTraceback Info:\n" + tbStr + "Error Info:    " + str(sys.exc_type)+ ": " + str(sys.exc_value) + "\n"
    # print messages for use in Python/PythonWin
    arcpy.AddError(pymsg)


if __name__ == "__main__":
    main()
