const amqp = require('amqplib');
const fs = require('fs');
const path = require('path')
const elastic = require('@elastic/elasticsearch');
const { parse } = require('csv-parse');
const dotenv = require('dotenv');

dotenv.config();

if(!fs.existsSync('./logs')) {
    fs.mkdirSync('./logs');
}

const client = new elastic.Client({
  node: process.env.ELASTICSEARCH_URL,
  auth: {
    username: process.env.ELASTICSEARCH_USERNAME,
    password: process.env.ELASTICSEARCH_PASSWORD
  }
})
function getFiles(dir, files = []) {
  
  const fileList = fs.readdirSync(dir)

  for (const file of fileList) {
    const name = `${dir}/${file}`

    if (fs.statSync(name).isDirectory()) {
     
      getFiles(name, files)
    } else {
      
      files.push(name)
    }
  }
  return files
}



async function refreshDB() {
  const files = getFiles('./logs')

  for (const file of files.filter((f) => !f.includes('archived'))) {
    fs.createReadStream(file)
  .pipe(parse({ delimiter: ",", from_line: 2 }))
    .on("data", async function (row) {
      await client.index({
        index: 'logs',
        body: {
          timestamp: row[0],
          level: row[1],
          message: row[2],
          context: row[3]
        }
      })
    }).on("close", function () {
      console.log(file.split('.')[1])
      const directory = path.join(path.dirname(file), 'archived')
      const filename = path.join(directory, `${path.basename(file).replace('.csv', '')}-${Date.now()}`)

      if (!fs.existsSync(directory)) {
        fs.mkdirSync(directory);
      }
      fs.renameSync(file, filename)
    })
   }
    console.log('Database refreshed')
    setTimeout(() => refreshDB(), 60000)
}


async function consumeLogs() {
  try {
    const connection = await amqp.connect(process.env.RABBITMQ_URL);
    const channel = await connection.createChannel();

    const queue = 'logs'; // Replace 'logs' with the name of your queue

    await channel.assertQueue(queue, { durable: true });

    console.log('Waiting for logs. To exit, press CTRL+C');
    
    channel.consume(queue, (msg) => {
      const log = JSON.parse(msg.content.toString());
      const ms = log.context
      const folderName = ms
      if(!fs.existsSync(`./logs/${folderName}`)) {
        fs.mkdirSync(`./logs/${folderName}`);
      }
      if(!fs.existsSync(`./logs/${folderName}/${new Date().toISOString().split('T')[0]}-logs.csv`)) {
        fs.appendFileSync(`./logs/${folderName}/${new Date().toISOString().split('T')[0]}-logs.csv`, 'timestamp,level,message,context\n');
      }
      fs.appendFileSync(`./logs/${folderName}/${new Date().toISOString().split('T')[0]}-logs.csv`, `${new Date(parseInt(log.timestamp)).toISOString()},${log.level},${log.message},${log.context}\n`);
      console.log(`Received log: ${msg.content.toString()}`);
      
    }, { noAck: true });
  } catch (error) {
    console.error('Error occurred:', error);
  }
}

consumeLogs();

refreshDB()