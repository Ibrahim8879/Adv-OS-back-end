const express = require('express');
const bodyParser = require('body-parser');
const multer = require('multer');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const zlib = require('zlib');
const crypto = require('crypto');

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

let AllSlaves = [];

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

const compressAndSendFileChunksToSlaves = async (filePath, filename, passwordHash) => {
  const compressedFilePath = `${filePath}.gz`;
  
  // Compress the file
  const fileContents = fs.createReadStream(filePath);
  const writeStream = fs.createWriteStream(compressedFilePath);
  const gzip = zlib.createGzip();
  fileContents.pipe(gzip).pipe(writeStream);

  await new Promise((resolve, reject) => {
    writeStream.on('finish', resolve);
    writeStream.on('error', reject);
  });

  const fileSize = fs.statSync(compressedFilePath).size;
  const stream = fs.createReadStream(compressedFilePath, { highWaterMark: CHUNK_SIZE });
  let chunkIndex = 0;
  const metadata = {
    filename,
    size: fileSize,
    chunks: [],
    passwordHash,
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
      await sendChunkToSlave(chunk, chunkFilename+passwordHash, slaveClient);
      chunkInfo.slaves.push(`${slaveIndex}`);
    }

    metadata.chunks.push(chunkInfo);
    chunkIndex++;
  }

  // Write metadata to file
  fs.writeFileSync(`uploads/${filename}_${passwordHash}.metadata.json`, JSON.stringify(metadata, null, 2));

  // Remove the compressed file after chunks are sent
  fs.unlinkSync(compressedFilePath);

  return metadata;
};

app.get('/', (req, res) => {
  res.send('Welcome to the P2P file storage backend!');
});

app.post('/upload', upload.single('file'), async (req, res) => {
  const file = req.file;
  const passwordHash = req.body.passwordHash; // Receive the hashed password
  if (!file || !passwordHash) {
    res.status(400).send('No file uploaded or password hash missing.');
    return;
  }

  try {
    const filePath = path.join('uploads', file.filename);
    const metadata = await compressAndSendFileChunksToSlaves(filePath, file.filename, passwordHash);
    res.send(`File uploaded successfully: ${file.filename}`);

    // Delete file after metadata is generated
    fs.unlinkSync(filePath);
  } catch (error) {
    console.error('Error sending file chunks:', error);
    res.status(500).send('Error uploading file.');
  }
});

const downloadChunkFromSlave = (slave, chunkFilename, passwordHash) => {
  return new Promise((resolve, reject) => {
    const call = slave.DownloadChunk({ filename: chunkFilename+passwordHash });
    console.log("getting in the chunk data");
    let chunkData = Buffer.alloc(0);
    call.on('data', (chunk) => {
      chunkData = Buffer.concat([chunkData, chunk.chunk]);
    });

    call.on('end', () => {
      resolve(chunkData);
    });

    call.on('error', (error) => {
      reject(error);
    });
  });
};

app.get('/download/:filename', async (req, res) => {
  const filename = req.params.filename;
  const passwordHash = req.headers['passwordhash'];
  const metadataPath = path.join('uploads', `${filename}_${passwordHash}.metadata.json`);
  console.log(metadataPath);
  if (fs.existsSync(metadataPath)) {
    try {
      const metadata = JSON.parse(fs.readFileSync(metadataPath));
      const chunksPromises = metadata.chunks.map(async (chunkInfo) => {
        const { chunkFilename, slaves } = chunkInfo;
        let chunkData = null;
        for (const slave of slaves) {
          console.log(`Downloading chunk ${chunkFilename} from slave ${slave}`);
          const slaveClient = AllSlaves[slave];
          try {
            console.log("getting chunk data from slave");
            chunkData = await downloadChunkFromSlave(slaveClient, chunkFilename, passwordHash);
            break; // Break loop if chunk is successfully downloaded from any slave
          } catch (error) {
            console.error(`Error downloading chunk ${chunkFilename} from ${slave}:`, error);
          }
        }

        if (!chunkData) {
          throw new Error(`Failed to download chunk ${chunkFilename}`);
        }

        return chunkData;
      });

      // Wait for all chunks to be downloaded
      const chunks = await Promise.all(chunksPromises);

      // Concatenate all chunks into a single buffer
      const compressedFileBuffer = Buffer.concat(chunks);

      // Set response headers for file download
      res.set({
        'Content-Type': 'application/octet-stream',
        'Content-Disposition': `attachment; filename="${filename}.gz"`
      });

      // Send compressed file buffer as response
      res.send(compressedFileBuffer);
    } catch (error) {
      console.error('Error downloading file:', error);
      res.status(500).send('Error downloading file.');
    }
  } else {
    res.status(404).send('File not found.');
  }
});

app.get('/files', (req, res) => {
  const userPasswordHash = req.headers['passwordhash'];
  fs.readdir('uploads/', (err, files) => {
    if (err) {
      console.error('Error reading directory:', err);
      res.status(500).send('Internal Server Error');
      return;
    }

    const filesMetadata = files
      .filter(file => file.endsWith('.metadata.json'))
      .filter(metadataFile => {
        const metadata = JSON.parse(fs.readFileSync(path.join('uploads', metadataFile)));
        // Assuming metadata contains a passwordHash field
        return metadata.passwordHash === userPasswordHash;
      })
      .map(metadataFile => {
        const metadata = JSON.parse(fs.readFileSync(path.join('uploads', metadataFile)));

        // Function to convert file size to appropriate unit (KB, MB, GB)
        const formatFileSize = (bytes) => {
          if (bytes < 1024) {
            return bytes + ' B';
          } else if (bytes < 1024 * 1024) {
            return (bytes / 1024).toFixed(2) + ' KB';
          } else if (bytes < 1024 * 1024 * 1024) {
            return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
          } else {
            return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
          }
        };

        // Convert file size to appropriate unit
        const sizeInAppropriateUnit = formatFileSize(metadata.size);

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


app.post('/register', (req, res) => {
  const { address } = req.body;
  if (!address) {
    res.status(400).send('Missing address field.');
    return;
  }

  const slaveClient = new fileTransferProto.FileTransfer(address, grpc.credentials.createInsecure(), {
    'grpc.max_receive_message_length': 100 * 1024 * 1024, // 100MB
    'grpc.max_send_message_length': 100 * 1024 * 1024, // 100MB
  });
  AllSlaves.push(slaveClient);
  res.send('Slave registered successfully.');
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});
