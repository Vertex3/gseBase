# makeBatFiles.py

import os, sys

dwgFolders = r'E:\Vault_Mirror\CADVault\UWS'

dirs = os.listdir(dwgFolders)

for dir in dirs:
	print dir
	batcontents = 'py\\gseLoaderFME.py fpLoadPlaylist.xml,fpDerivePlaylist.xml gseDataConfig' + dir + '.xml True False'
	filename = 'floorplan_sync' + dir + '.bat'
	f = open(filename,'w')
	f.write(batcontents)
	f.close()