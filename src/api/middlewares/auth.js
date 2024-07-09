const { ValidateSignature } = require("../../utils");

module.exports = async (req, res, next) => {
  const isAuthorized = await ValidateSignature(req);
  req.userAuthenticated = true;

  if (isAuthorized) {
    return next();
  }

  return res.status(401).json({ message: "Not Authorized" });
};
