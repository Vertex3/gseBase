# gseDrawing.py - Gizinta Sync Engine drawing functions - get ID values from drawing name and/or FME feature
# SG Jan 2014
# ---------------------------------------------------------------------------

import sys,os, time

namedelimiter = "_"

def getDrawing(feature):
	dwgName = feature.getAttribute("DRAWING")
	return dwgName

def getSiteID(feature):
    site = "MAIN"
    pth = feature.getAttribute("autocad_source_filename")
    site = getSiteIDFromPath(pth)
    return site

def getSiteIDFromPath(pth):
	site = None
	try:
		pth = pth.split(os.sep)
		if len(pth) > 3:
			site = pth[len(pth)-4] # this will get the site folder name, needs to be set up for specific file structure
	except:
		print "Error getting SiteID for:" + str(pth)
	return site

def getFloorIDFromPath(pth):
    global namedelimiter
    dwg = pth[pth.rfind(os.sep)+1:]
    site = getSiteIDFromPath(pth)
    floorID = site + namedelimiter + getBuildingFromName(dwg) + namedelimiter + getFloorCodeFromName(dwg)
    return floorID

def getBuildingIDFromPath(pth):
    global namedelimiter
    dwg = pth[pth.rfind(os.sep)+1:]
    site = getSiteIDFromPath(pth)
    buildingID = site + namedelimiter + getBuildingFromName(dwg)
    return buildingID

def getSiteplanID(feature):
    site = "MAIN"
    pth = feature.getAttribute("autocad_source_filename")
    try:
        pth = pth.split(os.sep)
        if len(pth) > 2:
            site = pth[len(pth)-3] # this will get the site folder name, needs to be set up for specific file structure
    except:
        print "Error getting SiteID for:" + str(pth)
    return site

def getSiteplanBuildingID(feature):
	try:
		building = getSiteplanID(feature) + namedelimiter + str(feature.getAttribute("FACNUM"))
	except:
		building = None
	return building

def getBuildingID(feature):
	try:
		building = getSiteID(feature) + namedelimiter + str(getBuilding(feature))
	except:
		building = None
	return building

def getBuilding(feature):
	dwgName = getDrawing(feature)
	try:
		building = dwgName[:4]
	except:
		building = None
	return building

def getFloor(feature):
	dwgName = getDrawing(feature)
	if dwgName.rfind(".dwg") > -1:
		dwgName = dwgName[:dwgName.rfind(".dwg")]
	floorNum = dwgName[len(dwgName)-2:]
	return floorNum

def getFloorID(feature):
    global namedelimiter
    dwgName = getDrawing(feature)
    floorID = getBuildingID(feature) + namedelimiter + getFloor(feature)
    return floorID

def getSpaceID(feature,attrname):
	global namedelimiter
	spaceID = getFloorID(feature) + namedelimiter + feature.getAttribute(attrname)
	return spaceID

def getBuildingFromName(dwg):
    building = ""
    if dwg and len(dwg) > 3:
        building = dwg[:4]
    return building

def getFloorFromName(dwg):
	global namedelimiter
	floor = getBuildingFromName(dwg)  + namedelimiter + getFloorCodeFromName(dwg)
	return floor

def getFloorCodeFromName(dwg):
    dwg = dwg.lower().replace(".dwg",'')
    floorNum = dwg[len(dwg)-2:]
    return floorNum

def getDrawingFromName(dwg):
    if dwg.lower().rfind(".dwg") > -1:
        dwg = dwg[:dwg.lower().rfind(".dwg")]
    return dwg

def defaultUserName():
	return "DataLoad"

def timer(input):
    return time.time() - input

def getDBTime():
    return getStrTime(time.localtime())

def getStrTime(timeVal):
    return time.strftime("%Y%m%d%H%M%S", timeVal)

def getTimeFromStr(timeStr):
    return time.strptime(timeStr,"%d/%m/%Y %I:%M:%S %p")

def getSensitivity(feature):
	return 'None'

def getAmenityLayer(layer):
	if layer == "MISC-FENCE":
		layer = "Fence"
	else:
		layer = "Other"
	return layer
