const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const qs = require("qs");
//const amqplib = require("amqplib");
const axios = require("axios");
const https = require("https");
const crypto = require("crypto");

const agent = new https.Agent({
  rejectUnauthorized: false, // ignore SSL certificate verification
  secureOptions: crypto.constants.SSL_OP_LEGACY_SERVER_CONNECT,
});

const {
  APP_SECRET,
} = require("../config");
const { GetConfigValue } = require("../../../config/service_log_sp");

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



let currentToken = {};
let retryCount = 0;
const maxRetries = 3; // Maksimum yeniden deneme sayısı

module.exports.GenerateOAuth2Token = async (props, getAccess = 0) => {
  console.log("currentToken", currentToken);

  // Eğer currentToken[props.scope] mevcut değilse başlat
  if (!currentToken[props.scope]) {
    currentToken[props.scope] = {};
  }

  // Token'ın geçerlilik süresini kontrol et
  if (getAccess === 0 && currentToken[props.scope].value && currentToken[props.scope].expiry > Date.now()) {
    return currentToken[props.scope].value;
  }

  const tokenUrl = await GetConfigValue("PALOMA_TOKEN_URL");
  const clientInformation = {
    grant_type: getAccess === 0 && currentToken?.[props.scope]?.value && currentToken?.[props.scope]?.expiry < Date.now() ? "refresh_token" : props.grant_type,
    scope: props.scope,
    client_id: props.client_id,
    client_secret: props.client_secret,
    username: props.username,
    password: props.password,
    refresh_token: getAccess === 0 && currentToken?.[props.scope]?.value && currentToken?.[props.scope]?.expiry < Date.now() ? currentToken?.[props.scope]?.refreshToken : ""
  };

  try {
    const response = await axios.post(
      tokenUrl,
      qs.stringify(clientInformation),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
        httpsAgent: agent, // Ensure this is correctly defined or configured
        debug: true, // Enable verbose logging if necessary
      }
    );

    console.log("tokenResponse", response);

    // Token bilgilerini currentToken içinde sakla
    currentToken[props.scope] = {
      value: response.data.access_token,
      expiry: Date.now() + (response.data.expires_in * 1000),
      refreshToken: response.data.refresh_token
    };

    console.log("currentToken", currentToken);
    retryCount = 0; // Reset retry count after successful token retrieval
    return response.data.access_token;
  } catch (error) {
    if (error.response) {
      const status = error.response.status;
      if (status !== 200 && status !== 201 && retryCount < maxRetries) {
        retryCount++;
        console.log(`Yeniden deneme ${retryCount}/${maxRetries}`);
        return await module.exports.GenerateOAuth2Token(props, 1);
      }
    } else {
      return await module.exports.GenerateOAuth2Token(props, 1);
    }
    console.error('Token alma hatası:', error);
    throw error;
  }
};