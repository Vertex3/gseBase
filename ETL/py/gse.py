# gse.py - Gizinta Sync Engine Supporting functions
# ---------------------------------------------------------------------------
# Created on: 2014.923 SG
#
# Description: Supporting functions
# ---------------------------------------------------------------------------

import os, sys

dbSchema = 'sde'

ospath = os.path.realpath(__file__)
gsepath = ospath[:ospath.rfind(os.sep+'ETL')]

print gsepath

fmeFolder = gsepath + "\\ETL\\fme\\"
configFolder = gsepath + "\\ETL\\config\\"
serverConfigFolder = gsepath + "\\ETL\\serverConfig\\"
etlFolder = gsepath + "\\ETL\\"
sdeConnFolder = gsepath + "\\ETL\\serverConfig"
pyFolder = gsepath + "\\ETL\\py\\"


if pyFolder not in sys.path:
	sys.path.insert(0, pyFolder)

if etlFolder not in sys.path:
    sys.path.insert(0, etlFolder)

if fmeFolder not in sys.path:
    sys.path.insert(0, fmeFolder)

