const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const PROTO_PATH = './file_transfer.proto';
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {});
const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
const fileTransferProto = protoDescriptor.FilePackage;

const SLAVE_DIR = 'slave1';
const MASTER_ADDRESS = 'http://localhost:5000';

let isRegistered = false;

const ensureDirectoryExists = (dir) => {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
};

const saveChunkToFile = (filename, chunk) => {
  const filePath = path.join(SLAVE_DIR, filename);
  fs.appendFileSync(filePath, chunk);
};

const sendFile = (call) => {
  call.on('data', (fileChunk) => {
    const { filename, chunk } = fileChunk;
    ensureDirectoryExists(SLAVE_DIR);
    saveChunkToFile(filename, chunk);
    call.write({ message: `Chunk received for ${filename}` });
  });

  call.on('end', () => {
    call.end();
  });
};

const downloadChunk = (call) => {
  const { filename } = call.request;
  const chunkFilename = path.join(SLAVE_DIR, filename);
  console.log(chunkFilename)
  if (fs.existsSync(chunkFilename)) {
    const chunkBuffer = fs.readFileSync(chunkFilename);
    call.write({ filename, chunk: chunkBuffer });
  } else {
    call.emit('error', new Error('Chunk not found'));
  }

  call.end();
};

const server = new grpc.Server({
  'grpc.max_receive_message_length': 100 * 1024 * 1024, // 100MB
  'grpc.max_send_message_length': 100 * 1024 * 1024, // 100MB
});

server.addService(fileTransferProto.FileTransfer.service, { SendFile: sendFile, DownloadChunk: downloadChunk });
server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {
  server.start();
  console.log('Slave gRPC server running at http://0.0.0.0:50051');
  attemptRegistration();
});

const attemptRegistration = () => {
  axios.post(`${MASTER_ADDRESS}/register`, { address: 'localhost:50051' })
    .then(response => {
      console.log('Registered with master:', response.data);
      isRegistered = true;
      checkMasterConnection();
    })
    .catch(error => {
      console.error('Error registering with master, retrying in 5 seconds:', error.message);
      setTimeout(attemptRegistration, 5000); // Retry after 5 seconds
    });
};

const checkMasterConnection = () => {
  setInterval(() => {
    axios.get(`${MASTER_ADDRESS}/`)
      .then(response => {
        if (!isRegistered) {
          console.log('Master is back online, attempting to register...');
          attemptRegistration();
        }
      })
      .catch(error => {
        if (isRegistered) {
          console.error('Lost connection to master, will attempt to re-register:', error.message);
          isRegistered = false;
          attemptRegistration();
        }
      });
  }, 1000); // Check connection every 10 seconds
};
