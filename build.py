#!/usr/bin/env python

import sys
import os


THIS_SCRIPT = os.path.dirname(os.path.realpath( __file__ ))

MAIN_CLASS = 'edu/cshl/schatz/jnomics/tools/JnomicsMain'

PATHS = {
    'SRC': "src/main/java",
    'OUT': "target",
    'LIB': "lib",
    'CLASSPATH': '',
    'OUT_JAR': 'jnomics-0.3.jar'
}

for p in PATHS.iterkeys():
        PATHS[p] = os.path.join(THIS_SCRIPT, PATHS[p])



def getLibraries():
    return filter(lambda x: x.endswith(".jar"), os.listdir(PATHS['LIB']))

def compile():

    LIBRARIES = getLibraries()

    CLASSPATH = ":".join(map(lambda x: os.path.join(PATHS['LIB'], x), LIBRARIES))
    
    SRC_FILE_TREE = []

    def vis(files,dirname,names):
        java_files = filter(lambda x: os.path.isfile(os.path.join(dirname,x)) and x.endswith(".java"), names)
        files += map(lambda x: os.path.join(dirname,x),java_files)
        return names

    os.path.walk(PATHS['SRC'], vis,SRC_FILE_TREE)

    SRC_FILES = " ".join( SRC_FILE_TREE ) 

    if not os.path.exists(PATHS['OUT']):
        os.mkdir(PATHS['OUT'])

    cmd = "javac -d %s -cp %s %s" % (PATHS['OUT'], CLASSPATH, SRC_FILES)
    print "Compiling %d files into %s" % (len(SRC_FILE_TREE), PATHS['OUT'])
    os.system(cmd)


def jar():
    if not os.path.exists(PATHS['OUT']):
        compile()

    out_jar = PATHS['OUT_JAR']
    if os.path.exists(out_jar):
        print "Found old jar: %s ... removing" % out_jar
        os.remove(out_jar)

    out_dir = PATHS['OUT']
        
    cmd = "jar -cfe %s %s -C %s ."  % (out_jar,MAIN_CLASS,out_dir)

    #make fat jar
    LIBRARIES = map(lambda x: os.path.join(PATHS['LIB'], x), getLibraries())
    os.system('rm -rf %s' % os.path.join(PATHS['OUT'],"META-INF"))
    for lib in LIBRARIES:
            os.system("unzip -qo %s -d %s" % (lib,PATHS['OUT']))
            os.system('rm -rf %s' % os.path.join(PATHS['OUT'],"META-INF"))
    
    print "Building Jar %s " % out_jar
    os.system(cmd)


def clean():
    p = PATHS['OUT']
    print "Removing: %s " % p
    os.system('rm -rf %s' % p )

    print "Removing %s " % PATHS['OUT_JAR']
    os.system('rm %s '  % PATHS['OUT_JAR'] )

TASKS = { 'compile' : compile,
          'clean' : clean,
          'help' : help,
          'jar' : jar
          }



def help():
    print "Available Tasks:"
    for i in TASKS.iterkeys():
        print i
    
if __name__ == "__main__":

    if not len(sys.argv) > 1:
        help();sys.exit()

    task = sys.argv[1]

    if not TASKS.has_key(task):
        print "UNKNOWN TASK: %s" % task
        print help()
        sys.exit()
    else:
        TASKS[task]()
    


