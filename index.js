const { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } = require("@aws-sdk/client-sqs");
const { S3Client, GetObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
const { exec } = require("child_process");
const fs = require("fs");
const path = require("path");
const cors = require("cors");
const express = require("express");
const mime = require("mime-types");
//heloo
const app = express();
app.use(cors({ origin: "*" }));
app.use("/trans", express.static("trans"));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

// Define resolutions
const RES = [
  { name: "144p", width: 256, height: 144 },
  { name: "240p", width: 426, height: 240 },
  { name: "360p", width: 640, height: 360 },
  { name: "480p", width: 854, height: 480 },
  { name: "720p", width: 1280, height: 720 },
  { name: "1080p", width: 1920, height: 1080 },
];

const s3 = new S3Client({
  region: "ap-south-1",
  credentials: {
    accessKeyId: "AKIAXQIP76MSZRZ6B6PS",
    secretAccessKey: "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",
  },
});

const client = new SQSClient({
  region: "ap-south-1",
  credentials: {
    accessKeyId: "AKIAXQIP76MSZRZ6B6PS",
    secretAccessKey: "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",
  },
});

async function init() {
  const cmd = new ReceiveMessageCommand({
    QueueUrl: "https://sqs.ap-south-1.amazonaws.com/515966497573/myfirstq",
    MaxNumberOfMessages: 10,
    WaitTimeSeconds: 10,
  });

  while (true) {
    try {
      const response = await client.send(cmd);

      if (!response.Messages || response.Messages.length === 0) {
        continue;
      }

      for (const message of response.Messages) {
        const { ReceiptHandle, Body } = message;

        let event;
        try {
          event = JSON.parse(Body);
        } catch (err) {
          continue;
        }

        if (event.Event === "s3:TestEvent") continue;

        for (const record of event.Records) {
          const bucketName = record.s3.bucket.name;
          const objectKey = record.s3.object.key;

          if (bucketName && objectKey) {
            const videoId = path.basename(objectKey, path.extname(objectKey));
            await processVideo(bucketName, objectKey, videoId);
            await deleteMessage(ReceiptHandle);
          }
        }
      }
    } catch (error) {
      console.error("Error receiving messages:", error);
    }
  }
}

async function deleteMessage(receiptHandle) {
  /*const deleteCmd = new DeleteMessageCommand({
    QueueUrl: "https://sqs.ap-south-1.amazonaws.com/515966497573/myfirstq",
    ReceiptHandle: receiptHandle,
  });

  try {
    await client.send(deleteCmd);
    console.log("Message deleted successfully.");
  } catch (error) {
    console.error("Error deleting message:", error);
  }*/
}

async function processVideo(bucket, key, videoId) {
  const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });

  try {
    const result = await s3.send(cmd);

    const videoDir = path.join("trans", videoId);
    if (!fs.existsSync(videoDir)) {
      fs.mkdirSync(videoDir, { recursive: true });
    }

    const videoPath = path.join(videoDir, path.basename(key));
    const writeStream = fs.createWriteStream(videoPath);

    result.Body.pipe(writeStream);

    writeStream.on("finish", async () => {
      await transcode(videoPath, videoDir, videoId);
      fs.unlinkSync(videoPath);
    });
  } catch (error) {
    console.error(`Error downloading file ${key}:`, error);
  }
}

async function transcode(inputFile, outputDir, videoId) {
  const promises = RES.map(({ name, width, height }) => {
    return new Promise((resolve, reject) => {
      const resolutionDir = path.join(outputDir, name);
      if (!fs.existsSync(resolutionDir)) {
        fs.mkdirSync(resolutionDir, { recursive: true });
      }

      const outputFile = path.join(resolutionDir, "index.m3u8");
      const segmentFile = path.join(resolutionDir, "segment%03d.ts");

      const cmd = `ffmpeg -i "${inputFile}" \
      -vf "scale=w=${width}:h=${height}" \
      -c:v libx264 -preset fast -crf 23 -b:v 800k \
      -c:a aac -b:a 128k \
      -force_key_frames "expr:gte(t,n_forced*2)" \
      -f hls \
      -hls_time 10 \
      -hls_playlist_type vod \
      -hls_segment_filename "${segmentFile}" \
      -start_number 0 "${outputFile}"`;

      exec(cmd, async (error) => {
        if (error) {
          console.error(`Error transcoding to ${name}:`, error);
          reject(error);
        } else {
          try {
            await uploadToS3(`${videoId}/${name}/index.m3u8`, outputFile);
            let files;
            try {
              files = await fs.promises.readdir(resolutionDir);
            } catch (error) {
              reject(error);
              return;
            }
              const uploadPromises = files
                .filter((file) => file.endsWith(".ts"))
                .map((file) => uploadToS3(`${videoId}/${name}/${file}`, path.join(resolutionDir, file)));
              await Promise.all(uploadPromises);
resolve()
            }
          catch (uploadError) {
            reject(uploadError);
          }
        }
      });
    });
  });

  try {
    await Promise.all(promises);
    await generateIndexFile(outputDir, videoId);
    console.log("All resolutions transcoded and uploaded.");
  } catch (error) {
    console.error("Error during transcoding:", error);
  }
}

async function generateIndexFile(outputDir, videoId) {
  const indexPath = path.join(outputDir, "index.m3u8");
  const indexContent = RES.map(({ name, width, height }) => {
    const resolutionPath = `${name}/index.m3u8`;
    const bandwidth = width * height * 2;
    return `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${width}x${height}\n${resolutionPath}`;
  }).join("\n");

  fs.writeFileSync(indexPath, `#EXTM3U\n${indexContent}`);
  console.log("Master playlist created:", indexPath);

  await uploadToS3(`${videoId}/index.m3u8`, indexPath);
  console.log("Master playlist uploaded to S3.");
}

async function uploadToS3(key, filePath) {
  const upload = {
    Bucket: "perm-ffmpeg-bucket",
    Key: key,
    Body: fs.createReadStream(filePath),
    ContentType: mime.lookup(filePath) || "application/octet-stream",
  };

  try {
    await s3.send(new PutObjectCommand(upload));
    console.log(`Uploaded file to S3: ${key}`);
  } catch (error) {
    console.error(`Error uploading ${key} to S3:`, error);
  }
}

init();

app.listen(3000, () => {
  console.log("Server running on port 3000");
});
