set -e

function getParameter()
{
	key=$1
	section=$2
	file=$3

	awk -F $key= '/\['$section'\]/{a=1}a==1&&/'$key'=/{print $2;exit}' $file
}

#arg1 is the key matched to replace, arg2 is the value, arg3 is file
function sedOnePara()
{
	key=$1
	value=$2
	file=$3
	
    sed -i '' "s&$key=.*&$key=$value&g" $file
}

function printParameters()
{
	echo "DEFAULT PROJECT: ${DEFAULT_PROJECT}"
	echo "DEFAULT ENDPOINT: ${DEFAULT_ENDPOINT}"
	echo "DEFAULT ACCESS ID: ${DEFAULT_ACCESS_ID}"
	echo "DEFAULT ACCESS KEY: ${DEFAULT_ACCESS_KEY}"
	echo "DEFAULT TUNNEL: ${DEFAULT_TUNNEL}"

	echo "SECURITY PROJECT: ${SECURITY_PROJECT}"
	echo "SECURITY EDNPOINT: ${SECURITY_EDNPOINT}"
	echo "SECURITY ACCESS ID: ${SECURITY_ACCESS_ID}"
	echo "SECURITY ACCESS KEY: ${SECURITY_ACCESS_KEY}"
	echo "HTTPS ENDPOINT: ${HTTPS_ENDPOINT}"
}

#arg1 is the config file name
function setParameters()
{
	SDK_CONFIG=$1
	echo $1
	
	sedOnePara default.project $DEFAULT_PROJECT $SDK_CONFIG
	sedOnePara default.endpoint $DEFAULT_ENDPOINT $SDK_CONFIG
	sedOnePara default.access.id $DEFAULT_ACCESS_ID $SDK_CONFIG
	sedOnePara default.access.key $DEFAULT_ACCESS_KEY $SDK_CONFIG
	sedOnePara default.tunnel.endpoint $DEFAULT_TUNNEL $SDK_CONFIG
	
	sedOnePara security.project $SECURITY_PROJECT $SDK_CONFIG
	sedOnePara security.endpoint $SECURITY_EDNPOINT $SDK_CONFIG
	sedOnePara security.access.id $SECURITY_ACCESS_ID $SDK_CONFIG
	sedOnePara security.access.key $SECURITY_ACCESS_KEY $SDK_CONFIG

	sedOnePara https.endpoint $HTTPS_ENDPOINT $SDK_CONFIG
}

if [ "$#" != "1" ]; then
    echo "Usage:"
    echo "sh run_test.sh /path/to/odps_config.ini"
    exit 1
fi

CONFIG_PATH=$1

DEFAULT_PROJECT=$(getParameter project default $CONFIG_PATH)
DEFAULT_ENDPOINT=$(getParameter endpoint default $CONFIG_PATH)
DEFAULT_ACCESS_ID=$(getParameter access.id default $CONFIG_PATH)
DEFAULT_ACCESS_KEY=$(getParameter access.key default $CONFIG_PATH)
DEFAULT_TUNNEL=$(getParameter tunnel default $CONFIG_PATH)

SECURITY_PROJECT=$(getParameter project security $CONFIG_PATH)
SECURITY_EDNPOINT=$(getParameter endpoint security $CONFIG_PATH)
SECURITY_ACCESS_ID=$(getParameter access.id security $CONFIG_PATH)
SECURITY_ACCESS_KEY=$(getParameter access.key security $CONFIG_PATH)

HTTPS_ENDPOINT=$(getParameter endpoint https $CONFIG_PATH)

printParameters

for i in `find . -name '*test.conf'`
do
  setParameters $i
done
########################

mvn -U clean test -fn
mkdir -p target/surefire-reports
for i in `find ./ -name 'TEST-*.xml'`
do
  cp $i target/surefire-reports
done

mkdir -p target/junit-report
mvn -N antrun:run

echo >> $CONFIG_PATH
echo >> $CONFIG_PATH
echo "################################################ CONFIG INFO #####################################################################" >> $CONFIG_PATH
echo "##################################################################################################################################" >> $CONFIG_PATH

cat $CONFIG_PATH

echo "RUN DONE"