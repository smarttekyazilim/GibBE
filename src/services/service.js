const sp = require("./sp");


class GibService {
  constructor() { }

  async gibGetEpkbb(req, res, next) {
    try {

      const response = await sp.gibGetEpkbb();
      if (response && response.RESPONSECODE === "000") {
        return {
          STATUS: "success",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
          DATA: response.DATA,
        };
      } else {
        return {
          STATUS: "error",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
        };
      }
    } catch (error) {
      return {
        STATUS: "error",
        RESPONSECODE: "999",
        RESPONSECODEDESC: "Beklenmeyen bir hata oluştu.",
        RESPONSECODEDESC: error.message + error.stack,
      };
    }
  }

  async gibUpdateEphpycni(req, res, next) {
    try {

      const response = await sp.gibUpdateEphpycni(req.body);
      if (response && response.RESPONSECODE === "000") {
        return {
          STATUS: "success",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
          DATA: response.DATA,
        };
      } else {
        return {
          STATUS: "error",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
        };
      }
    } catch (error) {
      return {
        STATUS: "error",
        RESPONSECODE: "999",
        RESPONSECODEDESC: "Beklenmeyen bir hata oluştu.",
        RESPONSECODEDESC: error.message + error.stack,
      };
    }
  }

  async gibGetEphpycni(req, res, next) {
    try {

      const response = await sp.gibGetEphpycni();
      if (response && response.RESPONSECODE === "000") {
        return {
          STATUS: "success",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
          DATA: response.DATA,
        };
      } else {
        return {
          STATUS: "error",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
        };
      }
    } catch (error) {
      return {
        STATUS: "error",
        RESPONSECODE: "999",
        RESPONSECODEDESC: "Beklenmeyen bir hata oluştu.",
        RESPONSECODEDESC: error.message + error.stack,
      };
    }
  }

  async gibUpdateEpkbb(req, res, next) {
    try {

      const response = await sp.gibUpdateEpkbb(req.body);
      if (response && response.RESPONSECODE === "000") {
        return {
          STATUS: "success",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
          DATA: response.DATA,
        };
      } else {
        return {
          STATUS: "error",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
        };
      }
    } catch (error) {
      return {
        STATUS: "error",
        RESPONSECODE: "999",
        RESPONSECODEDESC: "Beklenmeyen bir hata oluştu.",
        RESPONSECODEDESC: error.message + error.stack,
      };
    }
  }

  async gibGetYt(req, res, next) {
    try {

      const response = await sp.gibGetYt();
      if (response && response.RESPONSECODE === "000") {
        return {
          STATUS: "success",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
          DATA: response.DATA,
        };
      } else {
        return {
          STATUS: "error",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
        };
      }
    } catch (error) {
      return {
        STATUS: "error",
        RESPONSECODE: "999",
        RESPONSECODEDESC: "Beklenmeyen bir hata oluştu.",
        RESPONSECODEDESC: error.message + error.stack,
      };
    }
  }

  async gibUpdateYt(req, res, next) {
    try {

      const response = await sp.gibUpdateYt(req.body);
      if (response && response.RESPONSECODE === "000") {
        return {
          STATUS: "success",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
          DATA: response.DATA,
        };
      } else {
        return {
          STATUS: "error",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
        };
      }
    } catch (error) {
      return {
        STATUS: "error",
        RESPONSECODE: "999",
        RESPONSECODEDESC: "Beklenmeyen bir hata oluştu.",
        RESPONSECODEDESC: error.message + error.stack,
      };
    }
  }

  async gibGetMenu(req, res, next) {
    try {

      const { LANGUAGE } = req.body

      const response = await sp.gibGetMenu(LANGUAGE);
      if (response && response.RESPONSECODE === "000") {
        return {
          STATUS: "success",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
          DATA: response.DATA,
        };
      } else {
        return {
          STATUS: "error",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
        };
      }
    } catch (error) {

      return {
        STATUS: "error",
        RESPONSECODE: "999",
        RESPONSECODEDESC: "Beklenmeyen bir hata oluştu.",
        RESPONSECODEDESC: error.message + error.stack,
      };
    }
  }

  async gibGetError(req, res, next) {
    try {

      const response = await sp.gibGetError();
      if (response && response.RESPONSECODE === "000") {
        return {
          STATUS: "success",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
          DATA: response.DATA,
        };
      } else {
        return {
          STATUS: "error",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
        };
      }
    } catch (error) {
      return {
        STATUS: "error",
        RESPONSECODE: "999",
        RESPONSECODEDESC: "Beklenmeyen bir hata oluştu.",
        RESPONSECODEDESC: error.message + error.stack,
      };
    }
  }

  async gibInsertError(req, res, next) {
    try {
      const response = await sp.gibInsertError(req.body);
      if (response && response.RESPONSECODE === "000") {
        return {
          STATUS: "success",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
          DATA: response.DATA,
        };
      } else {
        return {
          STATUS: "error",
          RESPONSECODE: response.RESPONSECODE,
          RESPONSECODEDESC: response.RESPONSECODEDESC,
        };
      }
    } catch (error) {
      return {
        STATUS: "error",
        RESPONSECODE: "999",
        RESPONSECODEDESC: "Beklenmeyen bir hata oluştu.",
        RESPONSECODEDESC: error.message + error.stack,
      };
    }
  }
}

module.exports = GibService;
