import logging
from ConfigParser import ConfigParser
from os import path

config = ConfigParser()
config.read(path.join(path.dirname((path.dirname(__file__))), 'config/config.ini'))

logging.basicConfig(format='%(asctime)s - %(message)s')
logging.getLogger().setLevel(logging.INFO)

SLI_REVISION_DB = 'sli_revision'
SLI_CON_REV_DB = 'sli_consensus_revision'
SLI_VAACTUALS_REV_DB = 'sli_vaactauls_revision'
SLI_DB = 'sli'
NMV = 'nmv_revision'
SAR = 'sar_revision'

MYSQL_DB_URL = "mysql://{0}:{1}@{2}/{3}"
COCKROACH_DB_URL = 'cockroachdb://{0}@{1}:{2}/{3}'
MONGO_DB_URL = "mongodb://{0}:{1}/"

PRINT_FORMAT = "{message}: {time}"

FETCH_REV_DP_REGEX = '\<([a-zA-Z]+)_(.*?)\>'