# gzfme.py - Gizinta FME functions
# SG Jan 2014
# ---------------------------------------------------------------------------
# Copyright 2012-2014 Vertex3 Inc
# This work is licensed under the Creative Commons Attribution-ShareAlike 3.0 Unported License. To view a copy of this license, visit http://creativecommons.org/licenses/by-sa/3.0/ or send a letter to Creative Commons, 444 Castro Street, Suite 900, Mountain View, California, 94041, USA.

import sys,os,time, xml.dom.minidom, pprint, gseDrawing

namedelimiter = "_"

#fmedir = r"d:\apps\fme2013\fmeobjects\python27"
#if fmedir not in sys.path:
#	sys.path.append(fmedir)
#import fmeobjects

allSourceTypes = "GDBDataset CADDataset MapLayer"
debug = False # set this in startup program
logger = ""

def setDebug(pdebug):
	global debug
	pdebug = str(pdebug)
	if pdebug.lower() == 'true':
		debug = True
	else:
		debug = False
	return debug


def collect_text(node):
	# "A function that collects text inside 'node', returning that text."
	s = ""
	for child_node in node.childNodes:
		if child_node.nodeType == child_node.TEXT_NODE:
			s += child_node.nodeValue
		else:
			s += collect_text(child_node)
	return s

def getNodeValue(xmlDoc,nodeName):
	# get an xml node value
	node = xmlDoc.getElementsByTagName(nodeName)
	try:
		str = collect_text(node.item(0))
	except:
		str = ""
	return str

def calcValue(feature,attrs,calcString):
	# calculate a value based on fields and or other expressions
	global debug
	outVal = ""

	if calcString.find("|") > -1:
		calcList = calcString.split("|")
	elif calcString.find("!") > -1:
		calcList = calcString.split("!")
	else:
		calcList = []
		outVal = calcString

	for strVal in calcList:
		if strVal in attrs:
			outVal += str(feature.getAttribute(strVal))
		else:
			outVal += strVal
	try:
		if debug == True:
			logger.logMessageString("gz Calculate string: " +outVal)
		outVal = eval(outVal)
		if debug == True:
			logger.logMessageString("gz Calculate result: " +outVal)

	except:
		logger.logMessageString("gz Error calculating field values:" + outVal)

	return outVal

def isGizintaDocument(xmlDoc):
	GizintaNode = None
	try:
		GizintaNode = xmlDoc.getElementsByTagName("Gizinta")
	except:
		pass
	if GizintaNode:
	   retVal = True
	else:
	   retVal = False
	return retVal

def isPlaylistDocument(xmlDoc):
	PlaylistNode = None
	try:
		PlaylistNode = xmlDoc.getElementsByTagName("GizintaPlaylist")
	except:
		pass
	if PlaylistNode:
	   retVal = True
	else:
	   retVal = False
	return retVal

def getXmlElements(xmlFileName,elemName):
	global debug
	retDoc = []
	folder = xmlFileName[:xmlFileName.rfind(os.sep)+1]
	xmlDoc = xml.dom.minidom.parse(xmlFileName)

	if xmlFileName.count("/") > 0:
		folder = xmlFileName[:xmlFileName.rfind('/')+1]
	if isGizintaDocument(xmlDoc):
		retDoc = xmlDoc.getElementsByTagName(elemName)
	elif isPlaylistDocument(xmlDoc):
		docs = xmlDoc.getElementsByTagName("File")
		for doc in docs:
			fileName = collect_text(doc)
			theFile = os.path.join(folder,fileName)
			if os.path.exists(theFile):
				xmlDoc2 = xml.dom.minidom.parse(theFile)
				xmlNodes = xmlDoc2.getElementsByTagName(elemName)
				for node in xmlNodes:
					retDoc.append(node)
				if debug == True:
					logger.logMessageString("gz " + elemName + " node appended for " + theFile)
			else:
				logger.logMessageString(theFile + " does not exist, continuing...")
				logger.logMessageString(sys.path[0] + "...")
	else:
		retDoc = None
	return retDoc

def getDataset(xmlFileName,fme_feature_type,sourceTypes):
	# given a sourceName find the dataset for the target name
	# check the source and target names of the feature then return the correct dataset node for that Dataset
	global debug
	sourceDatasets = getSourceDatasets(xmlFileName,sourceTypes)
	#try:
	for node in sourceDatasets:
		sourceName = node.getAttributeNode("sourceName").nodeValue
		targetName = node.getAttributeNode("targetName").nodeValue
		if debug == True:
			logger.logMessageString("gz Dataset Node name " + str(sourceName) + ", fme_feature_type=" + fme_feature_type)
		if sourceName == fme_feature_type:
			datasets = getDatasets(xmlFileName)
			for dsNode in datasets:
				name = dsNode.getAttributeNode("name").nodeValue
				if debug == True:
					logger.logMessageString("gz Dataset Node name " + str(name))
				if name == targetName:
					dataset = dsNode
					return dataset

	#except:
	#	logger.logMessageString("gz Dataset Node not found")

	return None

def getDatasetQA(xmlFileName,fme_feature_type):
	global debug
	datasets = getDatasets(xmlFileName)
	#try:
	for dsNode in datasets:
		name = dsNode.getAttributeNode("name").nodeValue
		if debug == True:
			logger.logMessageString("gz Dataset Node name " + str(name) + ", fme_feature_type=" + fme_feature_type)
		if name == fme_feature_type:
			dataset = dsNode
			if debug == True:
				logger.logMessageString("gz Dataset Node name matched " + str(name))
			return dataset
	#except:
	#	dataset = None
	#	logger.logMessageString("gz DatasetQA Node not found")
	return dataset

def getWhereClause(xmlFileName,datasetName):
	global debug
	whereClause = ""
	for sourceType in sourceTypes:
		ds = getXmlElements(xmlFileName,sourceType)
		for node in ds:
			name = node.getAttributeNode("sourceName").nodeValue
			if name == datasetName:
				try:
					whereNode = node.getElementsByTagName("WhereClause")
					whereClause = collect_text(whereNode)
				except:
					whereClause = ""
	return whereClause

def getDatasets(xmlFileName):
	datasets = getXmlElements(xmlFileName,'Dataset')
	logger.logMessageString("gz len(datasets)=" + str(len(datasets)))
	return datasets

def getValueMaps(xmlFileName):
	valueMaps = getXmlElements(xmlFileName,'ValueMap')
	logger.logMessageString("gz len(valueMaps)=" + str(len(valueMaps)))
	return valueMaps

def getSourceDatasets(xmlFileName,sourceTypes):
	global debug
	sourceDatasets = []
	xmlDoc = xml.dom.minidom.parse(xmlFileName)
	sTypes = sourceTypes.split(' ')
	for sourceType in sTypes:
		ds = getXmlElements(xmlFileName,sourceType)
		for node in ds:
			sourceDatasets.append(node)
			if debug == True:
				logger.logMessageString("gz source name=" + node.getAttributeNode("sourceName").nodeValue)
	return sourceDatasets

def doImports(fmefolder):
	thisFolder = sys.path[0]
	pyFolder = fmefolder.replace(fmefolder[:fmefolder.rfind(os.sep)+1],"py")
	if pyFolder not in sys.path:
		sys.path.insert(0, pyFolder)
	if thisFolder not in sys.path:
		sys.path.insert(0, thisFolder)
	import gzfme,gseDrawing

def shutdown(Errors,ignore,numWritten,FME_MacroValues):

	logger.logMessageString("gz gzfme.shutdown")
	logger.logMessageString("gz Errors=" + str(Errors))
	logger.logMessageString("gz number of features written = " + str(numWritten))

	if numWritten == 0:
		Errors = True
		err = "*** gz Fatal QA errors encountered - nothing written ***"
		h = "*************************************************************************"
		logger.logMessageString(h)
		logger.logMessageString(h)
		logger.logMessageString(e)
		logger.logMessageString(h)
		logger.logMessageString(h)

	if Errors == True and  ignore.lower() == 'false':
		err = "gz Fatal QA errors encountered"
		#pp = pprint.PrettyPrinter(indent=4)
		#pp.pprint(FME_MacroValues)
		logger.logMessageString(err)
		raise Exception(err)
	else:
		if ignore == 'true':
			logger.logMessageString("gz IgnoreErrors parameter was set to ignore all QA errors")
		else:
			logger.logMessageString("gz No Fatal QA errors encountered ")



