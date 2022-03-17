async function setupPlugin({ jobs, global, config }) {
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
            path: `${config.pythonSupportFileUpload}`, // let's allow this to be configurable?
            overwrite: `true`,
        }),
    }

    const handle = await createFileForDBFS(createFileRequest, global)

    const uploadFileRequest = {
        ...global.options,
        body: JSON.stringify({
            handle: handle,
            data: `aW1wb3J0IHN5cwppbXBvcnQgcGFuZGFzIGFzIHBkCgpkZiA9IHBkLnJlYWRfY3N2KCcvZGJmcy97fScuZm9ybWF0KHN5cy5hcmd2WzJdKSwgc2VwPSd8JyxkZWxpbWl0ZXI9Tm9uZSkKZnJvbSBweXNwYXJrLnNxbC50eXBlcyBpbXBvcnQgKgoKZGVmIGVxdWl2YWxlbnRfdHlwZShmKToKICBpZiBmID09ICdkYXRldGltZTY0W25zXSc6IHJldHVybiBEYXRlVHlwZSgpCiAgZWxpZiBmID09ICdpbnQ2NCc6IHJldHVybiBMb25nVHlwZSgpCiAgZWxpZiBmID09ICdpbnQzMic6IHJldHVybiBJbnRlZ2VyVHlwZSgpCiAgZWxpZiBmID09ICdmbG9hdDY0JzogcmV0dXJuIEZsb2F0VHlwZSgpCiAgZWxzZTogcmV0dXJuIFN0cmluZ1R5cGUoKQoKZGVmIGRlZmluZV9zdHJ1Y3R1cmUoc3RyaW5nLCBmb3JtYXRfdHlwZSk6CiAgdHJ5OiB0eXBvID0gZXF1aXZhbGVudF90eXBlKGZvcm1hdF90eXBlKQogIGV4Y2VwdDogdHlwbyA9IFN0cmluZ1R5cGUoKQogIHJldHVybiBTdHJ1Y3RGaWVsZChzdHJpbmcsIHR5cG8pCgpkZWYgcGFuZGFzX3RvX3NwYXJrKGRmX3BhbmRhcyk6CiAgY29sdW1ucyA9IGxpc3QoZGZfcGFuZGFzLmNvbHVtbnMpCiAgdHlwZXMgPSBsaXN0KGRmX3BhbmRhcy5kdHlwZXMpCiAgc3RydWN0X2xpc3QgPSBbXQogIGZvciBjb2x1bW4sIHR5cG8gaW4gemlwKGNvbHVtbnMsIHR5cGVzKTogCiAgICBzdHJ1Y3RfbGlzdC5hcHBlbmQoZGVmaW5lX3N0cnVjdHVyZShjb2x1bW4sIHR5cG8pKQogIHBfc2NoZW1hID0gU3RydWN0VHlwZShzdHJ1Y3RfbGlzdCkKICByZXR1cm4gc3FsQ29udGV4dC5jcmVhdGVEYXRhRnJhbWUoZGZfcGFuZGFzLCBwX3NjaGVtYSkgCgoKc2RmID0gcGFuZGFzX3RvX3NwYXJrKGRmKQpwZXJtYW5lbnRfdGFibGVfbmFtZSA9IHN5cy5hcmd2WzFdCgp0cnk6CiAgICBzZGYud3JpdGUuc2F2ZUFzVGFibGUocGVybWFuZW50X3RhYmxlX25hbWUpCmV4Y2VwdDoKICAgIHNkZi53cml0ZS5pbnNlcnRJbnRvKHBlcm1hbmVudF90YWJsZV9uYW1lLG92ZXJ3cml0ZT1GYWxzZSkKCg==`,
        }),
    }

    await uploadFileForDBFS(uploadFileForRequest, global)

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

    console.log(properties)
    // only move prop to elements for the $autocapture action
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
        .filter((event) => global.eventsToIgnore.includes(event.event))
        .map(transformEventToRow)
        .map((row) => {
            const keys = Object.keys(row)
            const values = keys.map((key) => row[key])
            return values.join('|') + '\n'
        })

    rows = rows.join().split('\n')

    let data = null
    // this is an intentional change as testing storage with posthog is a little difficult to simulate
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
        if (isRetry) {
            throw new Error(`${method} request to ${url} failed.`)
        }
        const res = await fetchWithRetry(url, options, (method = method), (isRetry = true))
        return res
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

async function runEveryMinute({ jobs, global, storage, config, cache }) {
    let request = global.options
    let isDataNull = false
    let job_id = null
    request.body = JSON.stringify({
        path: `${config.fileName}`,
        overwrite: `true`,
    })

    const handle = await createFileForDBFS(request, global)
    console.log('handle', handle)

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
        console.log('data uploaded', request.body)
    }

    await storage.set('data', [])

    request.body = JSON.stringify({
        handle: handle,
    })

    await closeFileForDBFS(request, global)
    console.log('closed', request.body)

    if (isDataNull) {
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
        /// this section of code can be removed based on jobs should be deleted or not
        if (jobIdToDrop) {
            request.body = JSON.stringify({
                job_id: jobIdToDrop,
            })
            const response = await fetchWithRetry(`${global.url}/api/2.1/jobs/delete`, request, 'POST')
            const result = await response.json()
            console.log('result', result)
        }

        let response = await fetchWithRetry(`${global.url}/api/2.1/jobs/create`, request, 'POST')
        let result = await response.json()
        console.log('result', result)
        job_id = result.job_id
        console.log('job_id', job_id)

        request.body = JSON.stringify({
            job_id: job_id,
            python_params: [`${config.dbName}`, `${config.fileName}`],
        })

        response = await fetchWithRetry(`${global.url}/api/2.1/jobs/run-now`, request, 'POST')
        result = await response.json()
        console.log('result', result)
        await cache.set('job_id', job_id)
    }
}

module.exports = {
    setupPlugin,
    exportEvents,
    runEveryMinute,
    transformEventToRow,
}
