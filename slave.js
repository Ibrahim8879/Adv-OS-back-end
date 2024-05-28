const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const fs = require('fs');
const path = require('path');

const PROTO_PATH = './file_transfer.proto';
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {});
const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
const fileTransferProto = protoDescriptor.FilePackage;

const SLAVE_DIR = 'slave1';

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


const server = new grpc.Server();
server.addService(fileTransferProto.FileTransfer.service, { SendFile: sendFile});
server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {
  server.start();
  console.log('Slave gRPC server running at http://0.0.0.0:50051');
});