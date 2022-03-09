async function setupPlugin({ jobs, global, config }) {
  console.log(`Setting up the plugin`);
  global.url = `https://${config.DomainName}`;
  // //   console.log(global.url);
  global.options = {
    headers: {
      Authorization: `Bearer ${config.ApiKey}`,
      "Content-Type": "application/json",
    },
  };

  global.eventsToIgnore = (config.eventsToIgnore || "")
    .split(",")
    .map((v) => v.trim());
}

function transformEventToRow(fullEvent) {
  const {
    event,
    properties,
    $set,
    $set_once,
    distinct_id,
    team_id,
    site_url,
    now,
    sent_at,
    uuid,
    ...rest
  } = fullEvent;
  const ip = properties?.["$ip"] || fullEvent.ip;
  const timestamp =
    fullEvent.timestamp || properties?.timestamp || now || sent_at;
  let ingestedProperties = properties;
  let elements = [];

  // only move prop to elements for the $autocapture action
  if (event === "$autocapture" && properties?.["$elements"]) {
    const { $elements, ...props } = properties;
    ingestedProperties = props;
    elements = $elements;
  }

  return {
    event,
    distinct_id,
    team_id,
    ip,
    site_url,
    timestamp,
    uuid: uuid,
  };
}

async function exportEvents(events, { jobs, global, config, storage }) {
  let rows = events
    .filter((event) => {
      if (global.eventsToIgnore.includes(event.event)) {
        return false;
      }
      return true;
    })
    .map(transformEventToRow)
    .map((row) => {
      const keys = Object.keys(row);
      const values = keys.map((key) => row[key]);
      return values.join(",") + "\n";
    });
  rows = rows.join().split("\n");

  let data = await storage.get("data", null);
  if (data === null) {
    data = [];
  }
  rows.forEach((row) => {
    if (row.length >= 0) {
      if (row[0] === ",") {
        row = row.substring(1);
      }
      data.push(row);
    }
  });
  await storage.set("data", data);
}

async function fetchWithRetry(
  url,
  options = {},
  method = "GET",
  isRetry = false
) {
  try {
    const res = await fetch(url, { method: method, ...options });
    return res;
  } catch {
    if (isRetry) {
      throw new Error(`${method} request to ${url} failed.`);
    }
    const res = await fetchWithRetry(
      url,
      options,
      (method = method),
      (isRetry = true)
    );
    return res;
  }
}

async function createFileForDBFS(request, global) {
  const response = await fetchWithRetry(
    `${global.url}/api/2.0/dbfs/create`,
    request,
    "POST"
  );
  let result = await response.json();
  console.log("handle came here", result);
  return result.handle;
}

async function uploadFileForDBFS(request, global) {
  const response = await fetchWithRetry(
    `${global.url}/api/2.0/dbfs/add-block`,
    request,
    "POST"
  );
  let result = await response.json();
  return result;
}

async function closeFileForDBFS(request, global) {
  const response = await fetchWithRetry(
    `${global.url}/api/2.0/dbfs/close`,
    request,
    "POST"
  );
  let result = await response.json();
  return result;
}

async function runEveryMinute({ jobs, global, storage, config, cache }) {
  let request = global.options;

  /// java script present year
  let year = new Date().getFullYear();
  let month = new Date().getMonth() + 1;
  let day = new Date().getDate();
  let hour = new Date().getHours();
  let min = new Date().getMinutes();

  request.body = JSON.stringify({
    path: `/${config.dbName}/${year}/${month}/${day}/${hour}_${min}_file.csv`,
    overwrite: false,
  });

  let handle = await createFileForDBFS(request, global);
  console.log(handle);

  let data = await storage.get("data", null);
  if (data === null) {
    data = [];
  }

  for (let content of data) {
    console.log(content);

    const contentBase64 = Buffer.from(content).toString("base64") + "Cg==";
    console.log(contentBase64);
    let request = global.options;
    request.body = JSON.stringify({
      handle: handle,
      data: contentBase64,
    });
    let response = await uploadFileForDBFS(request, global);
    console.log("uploaded", response);
  }

  await storage.set("data", []);

  request.body = JSON.stringify({
    handle: handle,
  });

  let response = await closeFileForDBFS(request, global);
  console.log("closed", response);
}

module.exports = {
  setupPlugin,
  exportEvents,
  runEveryMinute,
};
