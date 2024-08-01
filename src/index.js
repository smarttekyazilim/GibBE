require("rootpath")();
const express = require("express");
const cors = require("cors");
const bodyParser = require("body-parser");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const cron = require('node-cron')
const limiter = rateLimit({
  windowMs: 1000, // 1 saniye
  max: 500, // Maksimum 500 istek
  message:
    "Saniyede çok fazla istek yapıyorsunuz. Lütfen daha sonra tekrar deneyin.",
});

require('dotenv').config()

const { createParquetFiles } = require('./services/sp')
const decryptRequestBody = require("./api/middlewares/decryptRequestBody");

// const PORT = require("./config");
const PORT = 6049;

const StartServer = async () => {
  const app = express();
  app.use(bodyParser.json({ limit: "50mb" }));
  app.use(bodyParser.urlencoded({ limit: "50mb", extended: true }));
  app.use(cors());
  app.use(express.static(__dirname + "/public"));
  app.use(helmet());
  app.use(limiter);
  app.use(decryptRequestBody);
  const baseUrl = "/api/v1/";

  app.use(`${baseUrl}gib`, require("./services/controller"));

  app.get("/heartbeat", (req, res) => {
    res.status(200).send("Server is alive and well!");
  });

  app.listen(PORT, () => {
    console.log(`GIB sunucusu ${PORT} portunda ayakta!`);
  })

  // app.listen(PORT).on("error", (err) => { });
};

StartServer();

const jobs = () => {
  cron.schedule('* * * * *', async() => {
    try {
        console.log('başladı')
        createParquetFiles()
    } catch (err) {
        console.log('hata ==> ', err)
    }
  })
}

// jobs();



