const PYTHON_SUPPORT_FILE_CONTENT = `import sys
import pandas as pd

df = pd.read_csv('/dbfs/{}'.format(sys.argv[2]), sep='|',delimiter=None)
from pyspark.sql.types import *

def equivalent_type(f):
  if f == 'datetime64[ns]': return DateType()
  elif f == 'int64': return LongType()
  elif f == 'int32': return IntegerType()
  elif f == 'float64': return FloatType()
  else: return StringType()

def define_structure(string, format_type):
  try: typo = equivalent_type(format_type)
  except: typo = StringType()
  return StructField(string, typo)

def pandas_to_spark(df_pandas):
  columns = list(df_pandas.columns)
  types = list(df_pandas.dtypes)
  struct_list = []
  for column, typo in zip(columns, types):
    struct_list.append(define_structure(column, typo))
  p_schema = StructType(struct_list)
  return sqlContext.createDataFrame(df_pandas, p_schema)


sdf = pandas_to_spark(df)
permanent_table_name = sys.argv[1]

try:
    sdf.write.saveAsTable(permanent_table_name)
except:
    sdf.write.insertInto(permanent_table_name,overwrite=False)`

async function setupPlugin({ global, config }) {
    global.url = `https://${config.DomainName}`

    global.options = {
        headers: {
            Authorization: `Bearer ${config.ApiKey}`,
            'Content-Type': 'application/json',
        },
    }

    const createFileRequest = {
        ...global.options,
        body: JSON.stringify({
            path: `${config.pythonSupportFileUpload}`,
            overwrite: `true`,
        }),
    }

    const handle = await createFileForDBFS(createFileRequest, global)

    const uploadFileRequest = {
        ...global.options,
        body: JSON.stringify({
            handle: handle,
            data: Buffer.from(PYTHON_SUPPORT_FILE_CONTENT).toString('base64')
        }),
    }

    await uploadFileForDBFS(uploadFileRequest, global)

    const closeFileRequest = {
        ...global.options,
        body: JSON.stringify({
            handle: handle,
        }),
    }

    await closeFileForDBFS(closeFileRequest, global)

    global.eventsToIgnore = (config.eventsToIgnore || '').split(',').map((v) => v.trim())
}

function transformEventToRow(fullEvent) {
    const { event, properties, $set, $set_once, distinct_id, team_id, site_url, now, sent_at, uuid, ...rest } =
        fullEvent
    const ip = properties?.['$ip'] || fullEvent.ip
    const timestamp = fullEvent.timestamp || properties?.timestamp || now || sent_at
    let ingestedProperties = null
    let elements = null

    if (event === '$autocapture' && properties?.['$elements']) {
        const { ...props } = properties

        elements = props
    }

    return {
        event,
        distinct_id,
        team_id,
        ip,
        site_url,
        timestamp,
        uuid: uuid,
        properties: JSON.stringify(ingestedProperties ?? {}),
        elements: JSON.stringify(elements ?? []),
        people_set: JSON.stringify($set ?? {}),
        people_set_once: JSON.stringify($set_once ?? {}),
    }
}

async function exportEvents(events, { global, storage }) {
    let rows = events
        .filter((event) => !global.eventsToIgnore.includes(event.event))
        .map(transformEventToRow)
        .map((row) => {
            const keys = Object.keys(row)
            const values = keys.map((key) => row[key])
            return values.join('|') + '\n'
        })

    rows = rows.join().split('\n')

    let data = null
    if (!global.tests) {
        data = await storage.get('data', null)
    }
    if (data === null) {
        data = []
    }

    rows.forEach((row) => {
        if (row.length >= 0) {
            if (row[0] === ',' || row[0] === '|') {
                row = row.substring(1)
            }
            data.push(row)
        }
    })
    if (!global.tests) {
        await storage.set('data', data)
    }
    return data
}

async function fetchWithRetry(url, options = {}, method = 'GET', isRetry = false) {
    try {
        const res = await fetch(url, { method: method, ...options })
        return res
    } catch {
        throw RetryError()
    }
}

async function createFileForDBFS(request, global) {
    const response = await fetchWithRetry(`${global.url}/api/2.0/dbfs/create`, request, 'POST')
    const result = await response.json()
    return result.handle
}

async function uploadFileForDBFS(request, global) {
    const response = await fetchWithRetry(`${global.url}/api/2.0/dbfs/add-block`, request, 'POST')
    await response.json()
}

async function closeFileForDBFS(request, global) {
    const response = await fetchWithRetry(`${global.url}/api/2.0/dbfs/close`, request, 'POST')
    await response.json()
}

async function runEveryMinute({ global, storage, config, cache }) {
    let request = global.options
    let isDataNull = false
    let job_id = null
    request.body = JSON.stringify({
        path: `${config.fileName}`,
        overwrite: `true`,
    })

    const handle = await createFileForDBFS(request, global)

    let data = await storage.get('data', null)
    if (!data) {
        data = []
    } else {
        data.unshift(
            `event|distinct_id|team_id|ip|site_url|timestamp|uuid|properties|elements|people_set|people_set_once`
        )
    }

    for (const content of data) {
        isDataNull = true
        const contentBase64 = Buffer.from(content).toString('base64') + 'Cg=='

        request = global.options
        request.body = JSON.stringify({
            handle: handle,
            data: contentBase64,
        })
        await uploadFileForDBFS(request, global)
    }

    await storage.set('data', [])

    request.body = JSON.stringify({
        handle: handle,
    })

    await closeFileForDBFS(request, global)

    if (isDataNull) {
        // This is buggy - we override this before the right request.
        request.body = JSON.stringify({
            name: 'A python job to push data into db',
            tasks: [
                {
                    task_key: 'python',
                    description: 'Extracts session data from events',
                    depends_on: [],
                    existing_cluster_id: `${config.clusterId}`,
                    spark_python_task: {
                        python_file: `dbfs:${config.pythonSupportFileUpload}`,
                    },
                    libraries: [],
                    timeout_seconds: 86400,
                    max_retries: 3,
                    min_retry_interval_millis: 2000,
                    retry_on_timeout: false,
                },
            ],
            job_clusters: [
                {
                    job_cluster_key: 'auto_scaling_cluster',
                    new_cluster: {
                        spark_version: '7.3.x-scala2.12',
                        node_type_id: 'i3.xlarge',
                        spark_conf: {
                            'spark.speculation': true,
                        },
                        aws_attributes: {
                            availability: 'SPOT',
                            zone_id: 'us-west-2a',
                        },
                        autoscale: {
                            min_workers: 2,
                            max_workers: 16,
                        },
                    },
                },
            ],
            timeout_seconds: 86400,
            max_concurrent_runs: 10,
            format: 'MULTI_TASK',
        })

        const jobIdToDrop = await cache.get('job_id')
        if (jobIdToDrop) {
            request.body = JSON.stringify({
                job_id: jobIdToDrop,
            })
            const response = await fetchWithRetry(`${global.url}/api/2.1/jobs/delete`, request, 'POST')
            const result = await response.json()
        }

        let response = await fetchWithRetry(`${global.url}/api/2.1/jobs/create`, request, 'POST')
        let result = await response.json()

        job_id = result.job_id

        request.body = JSON.stringify({
            job_id: job_id,
            python_params: [`${config.dbName}`, `${config.fileName}`],
        })

        response = await fetchWithRetry(`${global.url}/api/2.1/jobs/run-now`, request, 'POST')
        result = await response.json()

        await cache.set('job_id', job_id)
    }
}

module.exports = {
    setupPlugin,
    exportEvents,
    runEveryMinute,
    transformEventToRow,
}
