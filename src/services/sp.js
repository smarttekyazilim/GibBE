const oracledb = require("oracledb");
const { getConnection } = require("../../../config/config");
const { ParquetWriter, ParquetSchema } = require('parquetjs');

oracledb.autoCommit = true;
if (process.platform === "win32") {
  oracledb.initOracleClient({ libDir: "C:\\oracle\\instantclient_21_8" });
} else {
  oracledb.initOracleClient({ libdir: "/opt/oracle/instantclient_21_9" });
}


async function gibGetEpkbb() {
  const connection = await getConnection();

  const result = await connection.execute(
    `BEGIN GIB_EPKBB_GET_ALL(:O_RESPONSECODE, :O_RESPONSECODEDESC, :O_REC_LIST, :O_KURUM_CODE); END;`,
    {
      O_RESPONSECODE: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
      O_RESPONSECODEDESC: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
      O_REC_LIST: { dir: oracledb.BIND_OUT, type: oracledb.CURSOR },
      O_KURUM_CODE: { dir: oracledb.BIND_OUT, type: oracledb.STRING }
    }
  );

  const resultSet = result.outBinds.O_REC_LIST;
  const TRANSACTION_DATA = await resultSet.getRows();
  let metaData = resultSet.metaData;

  const columnNames = metaData.map(column => column.name);

  const formattedData = TRANSACTION_DATA.map(row => {
    const formattedRow = {};
    columnNames.forEach((columnName, index) => {
      formattedRow[columnName] = row[index];
    });
    return formattedRow;
  });

  await resultSet.close();
  await connection.close();

  let res = {};
  res.RESPONSECODE = result.outBinds.O_RESPONSECODE;
  res.RESPONSECODEDESC = result.outBinds.O_RESPONSECODEDESC;
  res.KURUM_KOD = result.outBinds.O_KURUM_CODE;
  res.DATA = formattedData;

  return res;
}

async function gibUpdateEpkbb(body) {

  const {RECORD_TYPE, L_REF, HSTK_VKN, HSTK_UNVAN, HSGK_AD, HSGK_SOYAD, HSGK_KIMLIK_TIPI, HSGK_KIMLIK_NO, HSGK_UYRUK, HSGK_ADRES, HSGK_ILCE_ADI, HSGK_POSTA_KOD, HSGK_IL_KOD, HSGK_IL_ADI, HS_TEL, HS_EPOSTA, HES_NO, DOVIZ_TIP, HSP_TIP, HSP_DURUM, HSP_ACLS_TAR, HSP_KPNS_TAR, HSP_BAKIYE, HSP_BAKIYE_TARIHI, HSP_KART_DURUM, HSP_KART_ACLS_TAR, HSP_KART_KPNS_TAR, HSP_KART_NO, KURUM_KOD, GNDRM_TARIHI, IS_SEND} = body

  const connection = await getConnection();

  const result = await connection.execute(
    `BEGIN GIB_EPKBB_UPDATE(:P_RECORD_TYPE, :P_L_REF, :P_HSTK_VKN, :P_HSTK_UNVAN, :P_HSGK_AD, :P_HSGK_SOYAD, :P_HSGK_KIMLIK_TIPI, :P_HSGK_KIMLIK_NO, :P_HSGK_UYRUK, :P_HSGK_ADRES, :P_HSGK_ILCE_ADI, :P_HSGK_POSTA_KOD, :P_HSGK_IL_KOD, :P_HSGK_IL_ADI, :P_HS_TEL, :P_HS_EPOSTA, :P_HES_NO, :P_DOVIZ_TIP, :P_HSP_TIP, :P_HSP_DURUM, :P_HSP_ACLS_TAR, :P_HSP_KPNS_TAR, :P_HSP_BAKIYE, :P_HSP_BAKIYE_TARIHI, :P_HSP_KART_DURUM, :P_HSP_KART_ACLS_TAR, :P_HSP_KART_KPNS_TAR, :P_HSP_KART_NO, :P_KURUM_KOD, :P_GNDRM_TARIHI, :P_IS_SEND, :O_RESPONSECODE, :O_RESPONSECODEDESC, :O_ERROR_DESCRIPTION ); END;`,
    {
      P_RECORD_TYPE: RECORD_TYPE,
      P_L_REF: L_REF,
      P_HSTK_VKN: HSTK_VKN,
      P_HSTK_UNVAN: HSTK_UNVAN,
      P_HSGK_AD: HSGK_AD,
      P_HSGK_SOYAD: HSGK_SOYAD,
      P_HSGK_KIMLIK_TIPI: HSGK_KIMLIK_TIPI,
      P_HSGK_KIMLIK_NO: HSGK_KIMLIK_NO,
      P_HSGK_UYRUK: HSGK_UYRUK,
      P_HSGK_ADRES: HSGK_ADRES,
      P_HSGK_ILCE_ADI: HSGK_ILCE_ADI,
      P_HSGK_POSTA_KOD: HSGK_POSTA_KOD,
      P_HSGK_IL_KOD: HSGK_IL_KOD,
      P_HSGK_IL_ADI: HSGK_IL_ADI,
      P_HS_TEL: HS_TEL,
      P_HS_EPOSTA: HS_EPOSTA,
      P_HES_NO: HES_NO,
      P_DOVIZ_TIP: DOVIZ_TIP,
      P_HSP_TIP: HSP_TIP,
      P_HSP_DURUM: HSP_DURUM,
      P_HSP_ACLS_TAR: HSP_ACLS_TAR,
      P_HSP_KPNS_TAR: HSP_KPNS_TAR,
      P_HSP_BAKIYE: HSP_BAKIYE,
      P_HSP_BAKIYE_TARIHI: HSP_BAKIYE_TARIHI,
      P_HSP_KART_DURUM: HSP_KART_DURUM,
      P_HSP_KART_ACLS_TAR: HSP_KART_ACLS_TAR,
      P_HSP_KART_KPNS_TAR: HSP_KART_KPNS_TAR,
      P_HSP_KART_NO: HSP_KART_NO,
      P_KURUM_KOD: KURUM_KOD,
      P_GNDRM_TARIHI: GNDRM_TARIHI,
      P_IS_SEND: IS_SEND,

      
      O_RESPONSECODE: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
      O_RESPONSECODEDESC: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
      O_ERROR_DESCRIPTION: { dir: oracledb.BIND_OUT, type: oracledb.STRING }
    }
  );

  await connection.close();

  let res = {};
  res.RESPONSECODE = result.outBinds.O_RESPONSECODE;
  res.RESPONSECODEDESC = result.outBinds.O_RESPONSECODEDESC;
  res.ERROR_DESCRIPTION = result.outBinds.O_ERROR_DESCRIPTION;

  return res;
}

async function createParquetEpkbb() {
  try {
    const { DATA, KURUM_KOD } = await gibGetEpkbb();

    const schema = new ParquetSchema({
      recordtype: { type: 'UTF8', optinal: true },
      lref: { type: 'UTF8', optinal: true },
      islemturu: { type: 'UTF8', optinal: true },
      hstkvkn: { type: 'UTF8', optinal: true },
      hstkunvan: { type: 'UTF8', optinal: true },
      hsgkad: { type: 'UTF8', optinal: true },
      hsgksoyad: { type: 'UTF8', optinal: true },
      hsgkkimliktipi: { type: 'UTF8', optinal: true },
      hsgkkimlikno: { type: 'UTF8', optinal: true },
      hsgkuyruk: { type: 'UTF8', optinal: true },
      hsgkadres: { type: 'UTF8'},
      hsgkilceadi: { type: 'UTF8', optinal: true },
      hsgkpostakod: { type: 'UTF8', optinal: true },
      hsgkilkod: { type: 'UTF8' },
      hsgkiladi: { type: 'UTF8', optinal: true },
      hstel: { type: 'UTF8', optinal: true },
      hseposta: { type: 'UTF8', optinal: true },
      hesno: { type: 'UTF8', optinal: true },
      doviztip: { type: 'UTF8', optinal: true },
      hsptip: { type: 'UTF8', optinal: true },
      hspdurum: {type: 'UTF8'},
      hspaclstar: { type: 'UTF8', optinal: true },
      hspkpnstar: { type: 'UTF8', optinal: true },
      hspbakiye: { type: 'UTF8', optinal: true },
      hspbakiyetarihi: { type: 'UTF8', optinal: true },
      hspkartdurum: { type: 'UTF8', optinal: true },
      hspkartaclstar: { type: 'UTF8', optinal: true },
      hspkartkpnstar: { type: 'UTF8', optinal: true },
      hspkartno: { type: 'UTF8', optinal: true },
      kurumkod: { type: 'UTF8', optinal: true },
    });
 
    const today = new Date();

    const year = today.getFullYear();
    const month = today.getMonth() + 1;
    const day = today.getDate();

    const writer = await ParquetWriter.openFile(schema, `${KURUM_KOD}_EPKBB_${year}_${month}_${day}_0002.parquet`);

    for (const record of DATA) {
      await writer.appendRow({
        recordtype: record.RECORD_TYPE || '',
        lref: record.L_REF || '',
        islemturu: record.ISLEM_TURU || '',
        hstkvkn: record.HSTK_VKN || '',
        hstkunvan: record.HSTK_UNVAN || '',
        hsgkad: record.HSGK_AD || '',
        hsgksoyad: record.HSGK_SOYAD || '',
        hsgkkimliktipi: record.HSGK_KIMLIK_TIPI || '',
        hsgkkimlikno: record.HSGK_KIMLIK_NO || '',
        hsgkuyruk: record.HSGK_UYRUK || '',
        hsgkadres: record.HSGK_ADRES || '',
        hsgkilceadi: record.HSGK_ILCE_ADI || '',
        hsgkpostakod: record.HSGK_POSTA_KOD || '',
        hsgkilkod: record.HSGK_IL_KOD.length > 1 ? '0'+ record.HSGK_IL_KOD : '00' + record.HSGK_IL_KOD || '',
        hsgkiladi: record.HSGK_IL_ADI || '',
        hstel: record.HS_TEL || '',
        hseposta: record.HS_EPOSTA || '',
        hesno: record.HES_NO || '',
        doviztip: record.DOVIZ_TIP || '',
        hsptip: record.HSP_TIP || '',
        hspdurum: record.HSP_DURUM || '',
        hspaclstar: record.HSP_ACLS_TAR.substring(0,8) || '',
        hspkpnstar: record.HSP_KPNS_TAR.substring(0,8) || '',
        hspbakiye: record.HSP_BAKIYE || '',
        hspbakiyetarihi: record.HSP_BAKIYE_TARIHI.substring(0,8) || '',
        hspkartdurum: record.HSP_KART_DURUM || '',
        hspkartaclstar: record.HSP_KART_ACLS_TAR.substring(0,8) || '',
        hspkartkpnstar: record.HSP_KART_KPNS_TAR.substring(0,8) || '',
        hspkartno: record.HSP_KART_NO || '',
        kurumkod: record.KURUM_KOD || '',
      });
    }
 
    await writer.close();
 
    console.log('Parquet file basariyla olusturuldu.');
  } catch (err) {
    console.error('Parquet file olusturulurken hata!: ', err);
  }
}

async function gibGetMenu(LANGUAGE) {
  const connection = await getConnection();

  const result = await connection.execute(
    `BEGIN GIB_MENU_GET(:P_LANGUAGE, :O_RESPONSECODE, :O_RESPONSECODEDESC, :O_REC_LIST); END;`,
    {
      P_LANGUAGE: LANGUAGE,
      O_RESPONSECODE: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
      O_RESPONSECODEDESC: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
      O_REC_LIST: { dir: oracledb.BIND_OUT, type: oracledb.CURSOR }
    }
  );

  const resultSet = result.outBinds.O_REC_LIST;
  const TRANSACTION_DATA = await resultSet.getRows();
  let metaData = resultSet.metaData;

  const columnNames = metaData.map(column => column.name);

  const formattedData = TRANSACTION_DATA.map(row => {
    const formattedRow = {};
    columnNames.forEach((columnName, index) => {
      formattedRow[columnName] = row[index];
    });
    return formattedRow;
  });

  await resultSet.close();
  await connection.close();

  let res = {};
  res.RESPONSECODE = result.outBinds.O_RESPONSECODE;
  res.RESPONSECODEDESC = result.outBinds.O_RESPONSECODEDESC;
  res.DATA = formattedData;

  return res;
}

async function gibGetError() {
  const connection = await getConnection();

  const result = await connection.execute(
    `BEGIN GIB_ERROR_GET(:O_RESPONSECODE, :O_RESPONSECODEDESC, :O_REC_LIST); END;`,
    {
      O_RESPONSECODE: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
      O_RESPONSECODEDESC: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
      O_REC_LIST: { dir: oracledb.BIND_OUT, type: oracledb.CURSOR }
    }
  );

  const resultSet = result.outBinds.O_REC_LIST;

  console.log('result ==> ', resultSet)

  const ERROR_DATA = await resultSet.getRows();
  let metaData = resultSet.metaData;

  const columnNames = metaData.map(column => column.name);

  const formattedData = ERROR_DATA.map(row => {
    const formattedRow = {};
    columnNames.forEach((columnName, index) => {
      formattedRow[columnName] = row[index];
    });
    return formattedRow;
  });

  await resultSet.close();
  await connection.close();

  let res = {};
  res.RESPONSECODE = result.outBinds.O_RESPONSECODE;
  res.RESPONSECODEDESC = result.outBinds.O_RESPONSECODEDESC;
  res.DATA = formattedData;

  return res;
}

async function gibInsertError(body) {

  const { FORM_TYPE, CODE, MESSAGE } = body

  const connection = await getConnection();
  const result = await connection.execute(
    `BEGIN GIB_ERROR_INSERT(:P_FORM_TYPE, :P_CODE, :P_MESSAGE, :O_RESPONSECODE, :O_RESPONSECODEDESC); END;`,
    {
      P_FORM_TYPE: FORM_TYPE,
      P_CODE: CODE,
      P_MESSAGE: MESSAGE,
      O_RESPONSECODE: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
      O_RESPONSECODEDESC: { dir: oracledb.BIND_OUT, type: oracledb.STRING },
    },
    {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
      autoCommit: true,
    }
  );

  let res = {};
  res.RESPONSECODE = result.outBinds.O_RESPONSECODE;
  res.RESPONSECODEDESC = result.outBinds.O_RESPONSECODEDESC;

  await connection.close();
  return res;
}


module.exports = {
  gibGetEpkbb,
  gibUpdateEpkbb,
  createParquetEpkbb,
  gibGetMenu,
  gibGetError,
  gibInsertError
};
