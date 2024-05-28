const express = require('express');
const bodyParser = require('body-parser');
const multer = require('multer');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

const PROTO_PATH = './file_transfer.proto';
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {});
const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
const fileTransferProto = protoDescriptor.FilePackage;

const app = express();

app.use(cors());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

const CHUNK_SIZE = 64 * 1024 * 1024; // 64MB

const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, 'uploads/');
  },
  filename: function (req, file, cb) {
    cb(null, file.originalname);
  }
});
const upload = multer({ storage: storage });

const AllSlaves = [
    new fileTransferProto.FileTransfer('localhost:50051', grpc.credentials.createInsecure()),
];


const sendChunkToSlave = (chunk, filename, slaveClient) => {
  return new Promise((resolve, reject) => {
    const call = slaveClient.SendFile();

    call.on('data', (response) => {
      console.log(response.message);
    });

    call.on('end', () => {
      resolve();
    });

    call.on('error', (error) => {
      reject(error);
    });

    call.write({ filename, chunk });
    call.end();
  });
};

const sendFileChunksToSlaves = async (filePath, filename) => {
  const fileSize = fs.statSync(filePath).size;
  const stream = fs.createReadStream(filePath, { highWaterMark: CHUNK_SIZE });
  let chunkIndex = 0;
  const metadata = {
    filename,
    size: fileSize,
    chunks: [],
  };

  for await (const chunk of stream) {
    const chunkFilename = `${filename}.chunk${chunkIndex}`;
    const chunkSlaveOrder = Array.from(Array(AllSlaves.length).keys());

    // Shuffle slave order to distribute redundancy
    for (let i = chunkSlaveOrder.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [chunkSlaveOrder[i], chunkSlaveOrder[j]] = [chunkSlaveOrder[j], chunkSlaveOrder[i]];
    }

    // Send chunk to each slave and record redundancy
    const chunkInfo = {
      chunkIndex,
      chunkFilename,
      slaves: []
    };

    for (const slaveIndex of chunkSlaveOrder) {
      const slaveClient = AllSlaves[slaveIndex];
      await sendChunkToSlave(chunk, chunkFilename, slaveClient);
      chunkInfo.slaves.push(`${slaveIndex}`);
    }

    metadata.chunks.push(chunkInfo);
    chunkIndex++;
  }

  // Write metadata to file
  fs.writeFileSync(`uploads/${filename}.metadata.json`, JSON.stringify(metadata, null, 2));

  return metadata;
};


app.get('/', (req, res) => {
  res.send('Welcome to the P2P file storage backend!');
});

app.post('/upload', upload.single('file'), async (req, res) => {
  const file = req.file;
  if (!file) {
    res.status(400).send('No file uploaded.');
    return;
  }

  try {
    const filePath = path.join('uploads', file.filename);
    const metadata = await sendFileChunksToSlaves(filePath, file.filename);
    res.send(`File uploaded successfully: ${file.filename}`);

    // Delete file after metadata is generated
    fs.unlinkSync(filePath);
  } catch (error) {
    console.error('Error sending file chunks:', error);
    res.status(500).send('Error uploading file.');
  }
});

app.get('/download/:filename', async (req, res) => {
  const filename = req.params.filename;
  const metadataPath = path.join('uploads', `${filename}.metadata.json`);

  if (fs.existsSync(metadataPath)) {
    try {
      const metadata = JSON.parse(fs.readFileSync(metadataPath));

      // chunks to be downloaded
      const chunks = await Promise.all(chunksPromises);

      // Concatenate all chunks into a single buffer
      const fileBuffer = Buffer.concat(chunks);

      // Set response headers for file download
      res.set({
        'Content-Type': 'application/octet-stream',
        'Content-Disposition': `attachment; filename="${filename}"`
      });

      // Send file buffer as response
      res.send(fileBuffer);
    } catch (error) {
      console.error('Error downloading file:', error);
      res.status(500).send('Error downloading file.');
    }
  } else {
    res.status(404).send('File not found.');
  }
});

app.get('/files', (req, res) => {
  fs.readdir('uploads/', (err, files) => {
    if (err) {
      console.error('Error reading directory:', err);
      res.status(500).send('Internal Server Error');
      return;
    }

    const filesMetadata = files
      .filter(file => file.endsWith('.metadata.json'))
      .map(metadataFile => {
        const metadata = JSON.parse(fs.readFileSync(path.join('uploads', metadataFile)));
        return {
          filename: metadata.filename,
          size: sizeInAppropriateUnit,
          numberOfChunks: metadata.chunks.length,
          chunks: metadata.chunks
        };
      });

    res.json(filesMetadata);
  });
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
