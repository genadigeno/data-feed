// Reads a datafeed feed: two pages, then a stream that survives dropped connections.
// Needs Node 18 or newer.
const server = process.env.DATAFEED_SERVER ?? "http://127.0.0.1:8080";
const token = process.env.DATAFEED_TOKEN;
const feed = process.argv[2];

async function get(path) {
  const headers = token ? { Authorization: `Bearer ${token}` } : {};
  const response = await fetch(`${server}/api/v1/feeds/${feed}/${path}`, { headers });
  if (!response.ok) throw new Error(`${response.status}: ${await response.text()}`);
  return response;
}

// Pages: carry on from X-Next-Offset, which is absent at the end.
let offset = "0";
for (let i = 0; i < 2 && offset !== null; i++) {
  const response = await get(`records?offset=${offset}&limit=3`);
  for (const line of (await response.text()).trim().split("\n")) {
    console.log("page:", JSON.parse(line).id);
  }
  offset = response.headers.get("X-Next-Offset");
}

// Stream: the position is the start plus the records received, so a dropped connection
// picks up exactly where it stopped. Stops here after 5 records.
let position = 0;
while (position < 5) {
  try {
    const response = await get(`stream?from=${position}&rate=10`);
    let partial = "";
    read: for await (const chunk of response.body.pipeThrough(new TextDecoderStream())) {
      const lines = (partial + chunk).split("\n");
      partial = lines.pop();
      for (const line of lines) {
        console.log("stream:", position, JSON.parse(line).id);
        position++;
        if (position === 5) break read;
      }
    }
  } catch (error) {
    console.log("reconnecting after:", error.message);
  }
}
