const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

const https = require("https");
const crypto = require("crypto");

const agent = new https.Agent({
  rejectUnauthorized: false, // ignore SSL certificate verification
  secureOptions: crypto.constants.SSL_OP_LEGACY_SERVER_CONNECT,
});

const {
  APP_SECRET,
} = require("../config");


//Utility functions
(module.exports.GenerateSalt = async () => {
  return await bcrypt.genSalt();
}),
  (module.exports.GeneratePassword = async (password, salt) => {
    return await bcrypt.hash(password, salt);
  });

module.exports.ValidatePassword = async (
  enteredPassword,
  savedPassword,
  salt
) => {
  return (await this.GeneratePassword(enteredPassword, salt)) === savedPassword;
};

(module.exports.GenerateSignature = async (payload) => {
  return await jwt.sign(payload, await APP_SECRET, { expiresIn: "1h" });
}),
  (module.exports.DecodeCustomer = async (req) => {
    const signature = req.get("Authorization");
    const payload = await jwt.verify(signature.split(" ")[1], await APP_SECRET);
    return payload;
  }),
  (module.exports.ValidateSignature = async (req) => {
    const signature = req.get("Authorization");

    if (signature) {
      try {
        const payload = await jwt.verify(signature.split(" ")[1], await APP_SECRET);
        req.user = payload;
        return true;
      } catch (error) {

        return false;
      }
    }

    return false;
  }),
  (module.exports.FormateData = (data) => {
    if (data) {
      return { data };
    } else {
      throw new Error("Data Not found!");
    }
  });

module.exports.Encrypt = async (txt) => {
  return await bcrypt.hash(`${txt}`, 10);
};
module.exports.Compare = async (txt, enc) => {
  return await bcrypt.compare(`${txt}`, enc);
};
module.exports.sha1 = async (data) => {
  //  
  return crypto.createHash("sha1").update(String(data), "binary").digest("hex");
};
