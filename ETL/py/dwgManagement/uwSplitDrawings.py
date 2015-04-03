import os, sys, errno, shutil


def main(argv = None):
    folder = r'C:\Apps\Gizinta\CADVault\Floorplans'
    outfolder = r'C:\Apps\Gizinta\CADVault'
    filegroup = 200
    dwgs = []
    dwg = []
    group = 0
    i = 0
    fcount = 0
    for root, dirs, files in os.walk(folder,followlinks=True):
        for file in files:
            i = i + 1
            newdir = makedirs(root,outfolder,group)
            docopy(root,newdir,file)
            fcount += 1
            if i > filegroup:
                if 'esri_cad.wld' in files:
                    docopy(root,newdir,'esri_cad.wld')
                    
                i = 0
                group = group + 1
                print 'Group', group

    print "processed", str(fcount),"drawings"

def docopy(root,newdir,file):
    srcname = os.path.join(root,file)
    dstname = os.path.join(newdir,file)
    try:
        shutil.copy2(srcname, dstname)
    except (IOError, os.error) as why:
        if not str(why).startswith('[Errno 13]'):
            print why, IOError, os.error
    

def makedirs(root,outfolder,group):
    dirs = root.replace(outfolder,"").split(os.sep)
    fpn = 'Floorplans' + str(group)
    newdir = root.replace(os.sep+ 'Floorplans' + os.sep,os.sep + fpn + os.sep)
    fldr = outfolder 
    for adir in dirs:
        if adir == 'Floorplans':
            adir = fpn
        fldr = fldr + os.sep + adir
        if not os.path.exists(fldr):
            os.mkdir(fldr)
    return newdir

##def copytree(src, dst, symlinks=False, ignore=None):
##    names = os.listdir(src)
##    if ignore is not None:
##        ignored_names = ignore(src, names)
##    else:
##        ignored_names = set()
##    try:
##        os.makedirs(dst)
##    except:
##        pass
##    errors = []
##    for name in names:
##        if name in ignored_names:
##            continue
##        srcname = os.path.join(src, name)
##        dstname = os.path.join(dst, name)
##        try:
##            if symlinks and os.path.islink(srcname):
##                linkto = os.readlink(srcname)
##                os.symlink(linkto, dstname)
##            elif os.path.isdir(srcname):
##                shutil.copytree(srcname, dstname, symlinks, ignore)
##            else:
##                shutil.copy2(srcname, dstname)
##            # XXX What about devices, sockets etc.?
##        except (IOError, os.error) as why:
##            errors.append((srcname, dstname, str(why)))
##        # catch the Error from the recursive copytree so that we can
##        # continue with other files
##        #except Error as err:
##        #    errors.extend(err.args[0])
##    try:
##        shutil.copystat(src, dst)
##    except WindowsError:
##        # can't copy file access times on Windows
##        pass
##    except OSError as why:
##        errors.extend((src, dst, str(why)))
##    #if errors:
##    #    raise Error(errors)


if __name__ == "__main__":
    main()
