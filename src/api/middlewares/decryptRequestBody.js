const crypto = require('crypto');
const key = Buffer.from('2862253705376133', 'utf8'); // 16 byte key
const iv = Buffer.from('8758273835876779', 'utf8'); // 16 byte IV
 
module.exports = async (req, res, next) => {
    try {
        if (req.method.toUpperCase() == "POST") {
            let decipher = crypto.createDecipheriv('aes-128-cbc', key, iv);
            let decrypted = decipher.update(req.body.ENCRYPTED_DATA, 'base64', 'utf8');
            decrypted += decipher.final('utf8');
            req.body = JSON.parse(decrypted);
            return next();
        } else {
            return next();
        }
    } catch (error) {
        console.log(error);
        return res.status(401).json({ message: "Not Authorized" });
    }
}