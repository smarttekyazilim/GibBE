const express = require("express");
const router = express.Router();
const services = require("./service");

const UserAuth = require("../api/middlewares/auth");

const routes = [
  // EK4
  { path: "/gibGetEpkbb", method: "get" },
  { path: "/gibUpdateEpkbb", method: "post" },

  // EK5
  { path: "/gibGetEphpycni", method: "get" },
  { path: "/gibUpdateEphpycni", method: "post" },

  { path: "/gibGetMenu", method: 'post'},
  { path: "/gibGetError", method: 'get'},
  { path: "/gibInsertError", method: 'post'},
];

const service = new services();

routes.forEach((route) => {
  const { path, method } = route;
  router[method](path, routeHandler);
});

async function routeHandler(req, res, next) {
  let servicePath = req.path.split("/")[1];

  await service[servicePath](req, res, next)
    .then(async (p) => {

      p ? res.status(200).send(p) : res.status(200).send(p);
    })
    .catch((error) => {
      next(error)
    });
}

module.exports = router;
