# Generated by Buildr 1.4.7, change to your liking


# Version number for this release
VERSION_NUMBER = "1.0.0"
# Group identifier for your projects
GROUP = "jnomics"
COPYRIGHT = ""

# Specify Maven 2.0 remote repositories here, like this:
repositories.remote << "http://repo1.maven.org/maven2"

COMMONS_COMPRESS = 'org.apache.commons:commons-compress:jar:1.4.1'
COMMONS_CLI = 'commons-cli:commons-cli:jar:1.2'
COMMONS_LANG3 = 'org.apache.commons:commons-lang3:jar:3.1'
SLF4J = 'org.slf4j:slf4j-api:jar:1.7.2'
HADOOP_CORE = 'org.apache.hadoop:hadoop-core:jar:1.1.0'
SAMJAR = 'net.sf.samtools:sam:jar:1.78'

download artifact(SAMJAR) => 
'https://downloads.sourceforge.net/project/picard/sam-jdk/1.78/sam-1.78.jar'


desc "The Jnomics project"
define "jnomics" do

  project.version = VERSION_NUMBER
  project.group = GROUP
  manifest["Implementation-Vendor"] = COPYRIGHT
  compile.with COMMONS_COMPRESS, COMMONS_CLI, COMMONS_LANG3, SLF4J, HADOOP_CORE, SAMJAR
  package(:jar)
end
