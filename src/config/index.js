const dotEnv = require("dotenv");
const path = require("path");

dotEnv.config({
  path: path.resolve(__dirname, `${process.env.NODE_ENV}.env`),
});
module.exports = {
  NODE_ENV: process.env.NODE_ENV || "prod",
  HOST: process.env.HOST || "185.99.199.122",
  PORT: process.env.PORT || 6049,

  USER_SERVICE: "user_service",
  EXCHANGE_NAME: process.env.EXCHANGE_NAME
};
