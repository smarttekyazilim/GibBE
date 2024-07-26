const oracledb = require("oracledb");
const fs = require('fs');
const path = require('path');
const { getConnection } = require("../../../config/config");
const { ParquetWriter, ParquetSchema } = require('parquetjs');
const Client = require('ssh2-sftp-client');

oracledb.autoCommit = true;
if (process.platform === "win32") {
  oracledb.initOracleClient({ libDir: "C:\\oracle\\instantclient_21_8" });
} else {
  oracledb.initOracleClient({ libdir: "/opt/oracle/instantclient_21_9" });
}

// EK4
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

  const {RECORD_TYPE, L_REF, HSTK_VKN, HSTK_UNVAN, HSGK_AD, HSGK_SOYAD, HSGK_KIMLIK_TIPI, HSGK_KIMLIK_NO, HSGK_UYRUK, HSGK_ADRES, HSGK_ILCE_ADI, HSGK_POSTA_KOD, HSGK_IL_KOD, HSGK_IL_ADI, HS_TEL, HS_EPOSTA, HES_NO, DOVIZ_TIP, HSP_TIP, HSP_DURUM, HSP_ACLS_TAR, HSP_KPNS_TAR, HSP_BAKIYE, HSP_BAKIYE_TARIHI, HSP_KART_DURUM, HSP_KART_ACLS_TAR, HSP_KART_KPNS_TAR, HSP_KART_NO, KURUM_KOD, GNDRM_TARIHI, IS_SEND, DELETED_FLAG } = body

  const connection = await getConnection();

  const result = await connection.execute(
    `BEGIN GIB_EPKBB_UPDATE(:P_RECORD_TYPE, :P_L_REF, :P_HSTK_VKN, :P_HSTK_UNVAN, :P_HSGK_AD, :P_HSGK_SOYAD, :P_HSGK_KIMLIK_TIPI, :P_HSGK_KIMLIK_NO, :P_HSGK_UYRUK, :P_HSGK_ADRES, :P_HSGK_ILCE_ADI, :P_HSGK_POSTA_KOD, :P_HSGK_IL_KOD, :P_HSGK_IL_ADI, :P_HS_TEL, :P_HS_EPOSTA, :P_HES_NO, :P_DOVIZ_TIP, :P_HSP_TIP, :P_HSP_DURUM, :P_HSP_ACLS_TAR, :P_HSP_KPNS_TAR, :P_HSP_BAKIYE, :P_HSP_BAKIYE_TARIHI, :P_HSP_KART_DURUM, :P_HSP_KART_ACLS_TAR, :P_HSP_KART_KPNS_TAR, :P_HSP_KART_NO, :P_KURUM_KOD, :P_GNDRM_TARIHI, :P_IS_SEND, :O_RESPONSECODE, :O_RESPONSECODEDESC, :O_ERROR_DESCRIPTION, :P_DELETED_FLAG ); END;`,
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
      P_DELETED_FLAG: DELETED_FLAG,

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

// EK5
async function gibGetEphpycni() {
  const connection = await getConnection();

  const result = await connection.execute(
    `BEGIN GIB_EPHPYCNI_GET_ALL(:O_RESPONSECODE, :O_RESPONSECODEDESC, :O_REC_LIST, :O_KURUM_CODE); END;`,
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

async function gibUpdateEphpycni(body) {

  const {L_REF, RECORD_TYPE, ISLEM_TURU, MUSTERI_MI, HESTK_VKN, HESTK_UNVAN, HESGK_AD, HESGK_SOYAD, HESGK_KIMLIK_TIPI, HESGK_KIMLIK_NO, HESGK_UYRUK, HESGK_ILCE_ADI, HESGK_POSTA_KOD, HESGK_IL_KOD, HESGK_IL_ADI, HES_TEL, HES_EPOSTA, HES_NO, DOVIZ_TIP, KISI_AD, KISI_SOYAD, KISI_KIMLIK_TIPI, KISI_KIMLIK_NO, IS_TAR, IS_KNL, ISLEM_TUTAR, ASIL_PARA_TUTAR, PARA_BIRIM, BRUT_KOM_TUT, TRX_ID, DELETED_FLAG, GNDRM_TAR, IS_SEND} = body

  const connection = await getConnection();

  const result = await connection.execute(
    `BEGIN GIB_EPHPYCNI_UPDATE(:P_L_REF, :P_RECORD_TYPE, :P_ISLEM_TURU, :P_MUSTERI_MI, :P_HESTK_VKN, :P_HESTK_UNVAN, :P_HESGK_AD, :P_HESGK_SOYAD, :P_HESGK_KIMLIK_TIPI, :P_HESGK_KIMLIK_NO, :P_HESGK_UYRUK, :P_HESGK_ILCE_ADI, :P_HESGK_POSTA_KOD, :P_HESGK_IL_KOD, :P_HESGK_IL_ADI, :P_HES_TEL, :P_HES_EPOSTA, :P_HES_NO, :P_DOVIZ_TIP, :P_KISI_AD, :P_KISI_SOYAD, :P_KISI_KIMLIK_TIPI, :P_KISI_KIMLIK_NO, :P_IS_TAR, :P_IS_KNL, :P_ISLEM_TUTAR, :P_ASIL_PARA_TUTAR, :P_PARA_BIRIM, :P_BRUT_KOM_TUT, :P_TRX_ID, :P_DELETED_FLAG, :P_GNDRM_TAR, :P_IS_SEND, :O_RESPONSECODE, :O_RESPONSECODEDESC, :O_ERROR_DESCRIPTION ); END;`,
    {
      P_L_REF: L_REF,
      P_RECORD_TYPE: RECORD_TYPE,
      P_ISLEM_TURU: ISLEM_TURU, 
      P_MUSTERI_MI: MUSTERI_MI, 
      P_HESTK_VKN: HESTK_VKN, 
      P_HESTK_UNVAN: HESTK_UNVAN, 
      P_HESGK_AD: HESGK_AD, 
      P_HESGK_SOYAD: HESGK_SOYAD, 
      P_HESGK_KIMLIK_TIPI: HESGK_KIMLIK_TIPI, 
      P_HESGK_KIMLIK_NO: HESGK_KIMLIK_NO, 
      P_HESGK_UYRUK: HESGK_UYRUK,  
      P_HESGK_ILCE_ADI: HESGK_ILCE_ADI, 
      P_HESGK_POSTA_KOD: HESGK_POSTA_KOD, 
      P_HESGK_IL_KOD: HESGK_IL_KOD, 
      P_HESGK_IL_ADI: HESGK_IL_ADI, 
      P_HES_TEL: HES_TEL, 
      P_HES_EPOSTA: HES_EPOSTA, 
      P_HES_NO: HES_NO, 
      P_DOVIZ_TIP: DOVIZ_TIP,
      P_KISI_AD: KISI_AD, 
      P_KISI_SOYAD: KISI_SOYAD, 
      P_KISI_KIMLIK_TIPI: KISI_KIMLIK_TIPI, 
      P_KISI_KIMLIK_NO: KISI_KIMLIK_NO, 
      P_IS_TAR: IS_TAR, 
      P_IS_KNL: IS_KNL, 
      P_ISLEM_TUTAR: ISLEM_TUTAR, 
      P_ASIL_PARA_TUTAR: ASIL_PARA_TUTAR, 
      P_PARA_BIRIM: PARA_BIRIM, 
      P_BRUT_KOM_TUT: BRUT_KOM_TUT, 
      P_TRX_ID: TRX_ID, 
      P_DELETED_FLAG: DELETED_FLAG, 
      P_GNDRM_TAR: GNDRM_TAR,
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

// EK6
async function gibGetYt() {
  const connection = await getConnection();

  const result = await connection.execute(
    `BEGIN GIB_YT_GET_ALL(:O_RESPONSECODE, :O_RESPONSECODEDESC, :O_REC_LIST, :O_KURUM_CODE); END;`,
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

async function gibUpdateYt(body) {

  const { L_REF, RECORD_TYPE, ISLEM_TURU, GON_MUSTERI_MI, GON_GK_AD, GON_GK_SOYAD, GON_GK_KIMLIK_TIPI, GON_GK_KIMLIK_NO, GON_ILCE_ADI, GON_IL_KOD, GON_IL_ADI, GON_TEL, GON_EPOSTA, GON_OK_EPARA, GON_BANKA_AD, GON_BANKA_KOD, GON_IBAN, GON_HES_NO, AL_MUSTERI_MI, AL_OK_VKN, AL_OK_UNVAN, AL_GK_AD, AL_GK_SOYAD, AL_GK_KIMLIK_NO, AL_ILCE_ADI, AL_IL_KOD, AL_IL_ADI, AL_TEL, AL_EPOSTA, AL_IBAN, AL_HES_NO, AL_KREDI_KART_NO, AL_DEBIT_KART_NO, IS_TAR, IS_SAAT, ODENME_TAR, ISLEM_IP, ISLEM_TUTAR, ASIL_TUTAR, PARA_BIRIM, BRUT_KOM_TUT, KUR_ACIKLAMA, MUS_ACIKLAMA, DELETED_FLAG, GNDRM_TAR, IS_SEND } = body

  const connection = await getConnection();

  const result = await connection.execute(
    `BEGIN GIB_YT_UPDATE(:P_L_REF, :P_RECORD_TYPE, :P_ISLEM_TURU, :P_GON_MUSTERI_MI, :P_GON_GK_AD, :P_GON_GK_SOYAD, :P_GON_GK_KIMLIK_TIPI, :P_GON_GK_KIMLIK_NO, :P_GON_ILCE_ADI, :P_GON_IL_KOD, :P_GON_IL_ADI, :P_GON_TEL, :P_GON_EPOSTA, :P_GON_OK_EPARA, :P_GON_BANKA_AD, :P_GON_BANKA_KOD, :P_GON_IBAN, :P_GON_HES_NO, :P_AL_MUSTERI_MI, :P_AL_OK_VKN, :P_AL_OK_UNVAN, :P_AL_GK_AD, :P_AL_GK_SOYAD, :P_AL_GK_KIMLIK_NO, :P_AL_ILCE_ADI, :P_AL_IL_KOD, :P_AL_IL_ADI, :P_AL_TEL, :P_AL_EPOSTA, :P_AL_IBAN, :P_AL_HES_NO, :P_AL_KREDI_KART_NO, :P_AL_DEBIT_KART_NO, :P_IS_TAR, :P_IS_SAAT, :P_ODENME_TAR, :P_ISLEM_IP, :P_ISLEM_TUTAR, :P_ASIL_TUTAR, :P_PARA_BIRIM, :P_BRUT_KOM_TUT, :P_KUR_ACIKLAMA, :P_MUS_ACIKLAMA, :P_DELETED_FLAG, :P_GNDRM_TAR, :P_IS_SEND, :O_RESPONSECODE, :O_RESPONSECODEDESC, :O_ERROR_DESCRIPTION ); END;`,
    {
      P_L_REF: L_REF,
      P_RECORD_TYPE: RECORD_TYPE,
      P_ISLEM_TURU: ISLEM_TURU,
      P_GON_MUSTERI_MI: GON_MUSTERI_MI, 
      P_GON_GK_AD: GON_GK_AD, 
      P_GON_GK_SOYAD: GON_GK_SOYAD, 
      P_GON_GK_KIMLIK_TIPI: GON_GK_KIMLIK_TIPI, 
      P_GON_GK_KIMLIK_NO: GON_GK_KIMLIK_NO, 
      P_GON_ILCE_ADI: GON_ILCE_ADI, 
      P_GON_IL_KOD: GON_IL_KOD,  
      P_GON_IL_ADI: GON_IL_ADI, 
      P_GON_TEL: GON_TEL, 
      P_GON_EPOSTA: GON_EPOSTA, 
      P_GON_OK_EPARA: GON_OK_EPARA, 
      P_GON_BANKA_AD: GON_BANKA_AD, 
      P_GON_BANKA_KOD: GON_BANKA_KOD, 
      P_GON_IBAN: GON_IBAN, 
      P_GON_HES_NO: GON_HES_NO,
      P_AL_MUSTERI_MI: AL_MUSTERI_MI, 
      P_AL_OK_VKN: AL_OK_VKN, 
      P_AL_OK_UNVAN: AL_OK_UNVAN, 
      P_AL_GK_AD: AL_GK_AD, 
      P_AL_GK_SOYAD: AL_GK_SOYAD, 
      P_AL_GK_KIMLIK_NO: AL_GK_KIMLIK_NO, 
      P_AL_ILCE_ADI: AL_ILCE_ADI, 
      P_AL_IL_KOD: AL_IL_KOD,
      P_AL_IL_ADI: AL_IL_ADI,
      P_AL_TEL: AL_TEL,
      P_AL_EPOSTA: AL_EPOSTA,
      P_AL_IBAN: AL_IBAN,
      P_AL_HES_NO: AL_HES_NO,
      P_AL_KREDI_KART_NO: AL_KREDI_KART_NO,
      P_AL_DEBIT_KART_NO: AL_DEBIT_KART_NO,
      P_IS_TAR: IS_TAR,
      P_IS_SAAT: IS_SAAT,
      P_ODENME_TAR: ODENME_TAR,
      P_ISLEM_IP: ISLEM_IP,
      P_ISLEM_TUTAR: ISLEM_TUTAR,
      P_ASIL_TUTAR: ASIL_TUTAR,
      P_ISLEM_IP: ISLEM_IP,
      P_ISLEM_IP: ISLEM_IP,
      P_PARA_BIRIM: PARA_BIRIM, 
      P_BRUT_KOM_TUT: BRUT_KOM_TUT, 
      P_KUR_ACIKLAMA: KUR_ACIKLAMA,
      P_MUS_ACIKLAMA: MUS_ACIKLAMA,
      P_KUR_ACIKLAMA: KUR_ACIKLAMA,
      P_DELETED_FLAG: DELETED_FLAG, 
      P_GNDRM_TAR: GNDRM_TAR,
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

// EK7
async function gibGetOkkib() {
  const connection = await getConnection();

  const result = await connection.execute(
    `BEGIN GIB_OKKIB_GET_ALL(:O_RESPONSECODE, :O_RESPONSECODEDESC, :O_REC_LIST, :O_KURUM_CODE); END;`,
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

async function gibUpdateOkkib(body) {
 
  const { L_REF, RECORD_TYPE, ISLEM_TURU, K_SAH_GK_AD, K_SAH_GK_SOYAD, K_SAH_GK_KIMLIK_TIPI, K_SAH_GK_KIMLIK_NO, K_SAH_KART_NO, BANK_TIP, BANK_EFT_KOD, BANK_ATM_KOD, IS_TAR, ISLEM_TUTAR, ASIL_PARA_TUTAR, PARA_BIRIM, BRUT_KOM_TUT, KURUM_KOD, DELETED_FLAG, GNDRM_TAR, IS_SEND, KUR_ACIKLAMA} = body

  const connection = await getConnection();

  const result = await connection.execute(
    `BEGIN GIB_OKKIB_UPDATE(:P_L_REF, :P_RECORD_TYPE, :P_ISLEM_TURU, :P_K_SAH_GK_AD, :P_K_SAH_GK_SOYAD, :P_K_SAH_GK_KIMLIK_TIPI, :P_K_SAH_GK_KIMLIK_NO, :P_K_SAH_KART_NO, :P_BANK_TIP, :P_BANK_EFT_KOD, :P_BANK_ATM_KOD, :P_IS_TAR, :P_ISLEM_TUTAR, :P_ASIL_PARA_TUTAR, :P_PARA_BIRIM, :P_BRUT_KOM_TUT, :P_KUR_ACIKLAMA, :P_IS_SEND, :P_KURUM_KOD, :P_DELETED_FLAG, :P_GNDRM_TAR, :O_RESPONSECODE, :O_RESPONSECODEDESC, :O_ERROR_DESCRIPTION ); END;`,
    {
      P_L_REF: L_REF,
      P_RECORD_TYPE: RECORD_TYPE,
      P_ISLEM_TURU: ISLEM_TURU, 
      P_K_SAH_GK_AD: K_SAH_GK_AD,
      P_K_SAH_GK_SOYAD: K_SAH_GK_SOYAD,
      P_K_SAH_GK_KIMLIK_TIPI: K_SAH_GK_KIMLIK_TIPI,
      P_K_SAH_GK_KIMLIK_NO: K_SAH_GK_KIMLIK_NO,
      P_K_SAH_KART_NO: K_SAH_KART_NO,
      P_BANK_TIP: BANK_TIP,
      P_BANK_EFT_KOD: BANK_EFT_KOD,
      P_BANK_ATM_KOD: BANK_ATM_KOD,
      P_IS_TAR: IS_TAR, 
      P_ISLEM_TUTAR: ISLEM_TUTAR, 
      P_ASIL_PARA_TUTAR: ASIL_PARA_TUTAR, 
      P_PARA_BIRIM: PARA_BIRIM, 
      P_BRUT_KOM_TUT: BRUT_KOM_TUT,
      P_KUR_ACIKLAMA: KUR_ACIKLAMA,
      P_IS_SEND: IS_SEND,
      P_KURUM_KOD: KURUM_KOD, 
      P_DELETED_FLAG: DELETED_FLAG, 
      P_GNDRM_TAR: GNDRM_TAR,
      
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

async function createParquetFiles() {
  try {
    const { DATA: epkbbData, KURUM_KOD: epkbbKurumKod } = await gibGetEpkbb();
    const { DATA: ephpycniData, KURUM_KOD: ephpycniKurumKod } = await gibGetEphpycni();
    const { DATA: ytData, KURUM_KOD: ytKurumKod } = await gibGetYt();
    const { DATA: okkibData, KURUM_KOD: okkibKurumKod } = await gibGetOkkib();

    const epkbbSchema = new ParquetSchema({
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
      hsgkadres: { type: 'UTF8', optinal: true},
      hsgkilceadi: { type: 'UTF8', optinal: true },
      hsgkpostakod: { type: 'UTF8', optinal: true },
      hsgkilkod: { type: 'UTF8', optinal: true },
      hsgkiladi: { type: 'UTF8', optinal: true },
      hstel: { type: 'UTF8', optinal: true },
      hseposta: { type: 'UTF8', optinal: true },
      hesno: { type: 'UTF8', optinal: true },
      doviztip: { type: 'UTF8', optinal: true },
      hsptip: { type: 'UTF8', optinal: true },
      hspdurum: {type: 'UTF8', optinal: true},
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

    const ephpycniSchema = new ParquetSchema({
      recordtype: { type: 'UTF8', optinal: true },
      lref: { type: 'UTF8', optinal: true },
      islemturu: { type: 'UTF8', optinal: true },
      musterimi: { type: 'UTF8', optinal: true },
      islemturu: { type: 'UTF8', optinal: true },
      hestkvkn: { type: 'UTF8', optinal: true },
      hestkunvan: { type: 'UTF8', optinal: true },
      hesgkad: { type: 'UTF8', optinal: true },
      hesgksoyad: { type: 'UTF8', optinal: true },
      hesgkkimliktipi: { type: 'UTF8', optinal: true },
      hesgkkimlikno: { type: 'UTF8', optinal: true },
      hesgkuyruk: { type: 'UTF8', optinal: true },
      hesgkadres: { type: 'UTF8', optinal: true},
      hesgkilceadi: { type: 'UTF8', optinal: true },
      hesgkpostakod: { type: 'UTF8', optinal: true },
      hesgkilkod: { type: 'UTF8', optinal: true },
      hesgkiladi: { type: 'UTF8', optinal: true },
      hestel: { type: 'UTF8', optinal: true },
      heseposta: { type: 'UTF8', optinal: true },
      hesno: { type: 'UTF8', optinal: true },
      doviztip: { type: 'UTF8', optinal: true },
      hsptip: { type: 'UTF8', optinal: true },
      kisiad: { type: 'UTF8', optinal: true },
      kisisoyad: { type: 'UTF8', optinal: true },
      kisikimliktipi: { type: 'UTF8', optinal: true },
      kisikimlikno: { type: 'UTF8', optinal: true },
      istar: { type: 'UTF8', optinal: true },
      isknl: { type: 'UTF8', optinal: true },
      bankaad: { type: 'UTF8', optinal: true },
      islemtutar: { type: 'UTF8', optinal: true },
      asilparatutar: { type: 'UTF8', optinal: true },
      parabirim: { type: 'UTF8', optinal: true },
      brutkomtut: { type: 'UTF8', optinal: true },
      musaciklama: { type: 'UTF8', optinal: true },
      kuraciklama: { type: 'UTF8', optinal: true },
      kurumkod: { type: 'UTF8', optinal: true },
    });

    const ytSchema = new ParquetSchema({
      recordtype: { type: 'UTF8', optinal: true },
      lref: { type: 'UTF8', optinal: true },
      islemturu: { type: 'UTF8', optinal: true },
      gonmusterimi: { type: 'UTF8', optinal: true },
      gonokvkn: { type: 'UTF8', optinal: true },
      gonokunvan: { type: 'UTF8', optinal: true },
      gontkvkn: { type: 'UTF8', optinal: true },
      gontkunvan: { type: 'UTF8', optinal: true },
      gongkad: { type: 'UTF8', optinal: true },
      gongksoyad: { type: 'UTF8', optinal: true },
      gongkkimliktipi: { type: 'UTF8', optinal: true },
      gongkkimlikno: { type: 'UTF8', optinal: true },
      gongkuyruk: { type: 'UTF8', optinal: true },
      gonadres: { type: 'UTF8', optinal: true },
      gonilceadi: { type: 'UTF8', optinal: true },
      gonpostakod: { type: 'UTF8', optinal: true },
      gonilkod: { type: 'UTF8', optinal: true },
      goniladi: { type: 'UTF8', optinal: true },
      gontel: { type: 'UTF8', optinal: true },
      goneposta: { type: 'UTF8', optinal: true },
      gonokhesno: { type: 'UTF8', optinal: true },
      gonokepara: { type: 'UTF8', optinal: true },
      gonokkartno: { type: 'UTF8', optinal: true },
      gonbankaad: { type: 'UTF8', optinal: true },
      gonbankakod: { type: 'UTF8', optinal: true },
      gonsubead: { type: 'UTF8', optinal: true },
      goniban: { type: 'UTF8', optinal: true },
      gonhesno: { type: 'UTF8', optinal: true },
      gonkredikartno: { type: 'UTF8', optinal: true },
      gondebitkartno: { type: 'UTF8', optinal: true },
      almusterimi: { type: 'UTF8', optinal: true },
      alokvkn: { type: 'UTF8', optinal: true },
      alokunvan: { type: 'UTF8', optinal: true },
      altkvkn: { type: 'UTF8', optinal: true },
      altkunvan: { type: 'UTF8', optinal: true },
      algkad: { type: 'UTF8', optinal: true },
      algksoyad: { type: 'UTF8', optinal: true },
      algkkimliktipi: { type: 'UTF8', optinal: true },
      algkkimlikno: { type: 'UTF8', optinal: true },
      algkuyruk: { type: 'UTF8', optinal: true },
      aladres: { type: 'UTF8', optinal: true },
      alilceadi: { type: 'UTF8', optinal: true },
      alpostakod: { type: 'UTF8', optinal: true },
      alilkod: { type: 'UTF8', optinal: true },
      aliladi: { type: 'UTF8', optinal: true },
      altel: { type: 'UTF8', optinal: true },
      aleposta: { type: 'UTF8', optinal: true },
      alokhesno: { type: 'UTF8', optinal: true },
      alokepara: { type: 'UTF8', optinal: true },
      alokkartno: { type: 'UTF8', optinal: true },
      albankaad: { type: 'UTF8', optinal: true },
      albankakod: { type: 'UTF8', optinal: true },
      alsubead: { type: 'UTF8', optinal: true },
      aliban: { type: 'UTF8', optinal: true },
      alhesno: { type: 'UTF8', optinal: true },
      alkredikartno: { type: 'UTF8', optinal: true },
      aldebitkartno: { type: 'UTF8', optinal: true },
      istar: { type: 'UTF8', optinal: true },
      issaat: { type: 'UTF8', optinal: true },
      odenmetar: { type: 'UTF8', optinal: true },
      islemip: { type: 'UTF8', optinal: true },
      islemtutar: { type: 'UTF8', optinal: true },
      asiltutar: { type: 'UTF8', optinal: true },
      parabirim: { type: 'UTF8', optinal: true },
      brutkomtut: { type: 'UTF8', optinal: true },
      isgonnedeni: { type: 'UTF8', optinal: true },
      islemknl: { type: 'UTF8', optinal: true },
      subevkn: { type: 'UTF8', optinal: true },
      subeunvan: { type: 'UTF8', optinal: true },
      subeiladi: { type: 'UTF8', optinal: true },
      kuraciklama: { type: 'UTF8', optinal: true },
      musaciklama: { type: 'UTF8', optinal: true },
      kurumkod: { type: 'UTF8', optinal: true }
    })

    const okkibSchema = new ParquetSchema({
      recordtype: { type: 'UTF8', optinal: true },
      lref: { type: 'UTF8', optinal: true },
      islemturu: { type: 'UTF8', optinal: true },
      ksahtkvkn: { type: 'UTF8', optinal: true },
      ksahtkunvan: { type: 'UTF8', optinal: true },
      ksahgkad: { type: 'UTF8', optinal: true },
      ksahgksoyad: { type: 'UTF8', optinal: true },
      ksahgkkimliktipi: { type: 'UTF8', optinal: true },
      ksahgkkimlikno: { type: 'UTF8', optinal: true },
      ksahkartno: { type: 'UTF8', optinal: true },
      banktip: { type: 'UTF8', optinal: true },
      bankeftkod: { type: 'UTF8', optinal: true },
      bankatmkod: { type: 'UTF8', optinal: true },
      istar: { type: 'UTF8', optinal: true },
      islemtutar: { type: 'UTF8', optinal: true },
      asiltutar: { type: 'UTF8', optinal: true },
      parabirim: { type: 'UTF8', optinal: true },
      brutkomtut: { type: 'UTF8', optinal: true },
      musaciklama: { type: 'UTF8', optinal: true },
      kuraciklama: { type: 'UTF8', optinal: true },
      kurumkod: { type: 'UTF8', optinal: true }
    });
 
    const today = new Date();

    const year = today.getFullYear();
    const month = today.getMonth() + 1;
    const day = today.getDate();

    // EK4

    const writerEpkbb = await ParquetWriter.openFile(epkbbSchema, `/home/deuslu/api/Uat/gib/src/parquetFiles/epkbb/${epkbbKurumKod}_EPKBB_${year}_${month}_${day}_0004.parquet`);

    for (const record of epkbbData) {
      await writerEpkbb.appendRow({
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
        hspaclstar: record.HSP_ACLS_TAR?.substring(0,8) || '',
        hspkpnstar: record.HSP_KPNS_TAR?.substring(0,8) || '',
        hspbakiye: record.HSP_BAKIYE || '',
        hspbakiyetarihi: record.HSP_BAKIYE_TARIHI?.substring(0,8) || '',
        hspkartdurum: record.HSP_KART_DURUM || '',
        hspkartaclstar: record.HSP_KART_ACLS_TAR?.substring(0,8) || '',
        hspkartkpnstar: record.HSP_KART_KPNS_TAR?.substring(0,8) || '',
        hspkartno: record.HSP_KART_NO || '',
        kurumkod: record.KURUM_KOD || '',
      });
    }
 
    await writerEpkbb.close();

    // EK5

    const writerEphpycni = await ParquetWriter.openFile(ephpycniSchema, `/home/deuslu/api/Uat/gib/src/parquetFiles/ephpycni/${ephpycniKurumKod}_EPHPYCNI_${year}_${month}_${day}_0004.parquet`);

    for (const record of ephpycniData) {
      await writerEphpycni.appendRow({
        recordtype: record.RECORD_TYPE || '',
        lref: record.L_REF || '',
        islemturu: record.ISLEM_TURU || '',
        hestkvkn: record.HESTK_UNVAN || '',
        musterimi: record.MUSTERI_MI || '',
        hestkvkn: record.HESTK_VKN || '',
        hesgkad: record.HESGK_AD || '',
        hestkunvan: record.HESTK_UNVAN || '',
        hesgkad: record.HESGK_AD || '',
        hesgksoyad: record.HESGK_SOYAD || '',
        hesgkkimliktipi: record.HESGK_KIMLIK_TIPI || '',
        hesgkkimlikno: record.HESGK_KIMLIK_NO || '',
        hesgkuyruk: record.HESGK_UYRUK || '',
        hesgkadres: record.HESGK_ADRES || '',
        hesgkilceadi: record.HESGK_ILCE_ADI || '',
        hesgkpostakod: record.HESGK_POSTA_KOD || '', 
        hesgkilkod: record.HESGK_IL_KOD && (record.HESGK_IL_KOD.length > 1 ? '0' + record.HESGK_IL_KOD : '00' + record.HESGK_IL_KOD) || '',
        hesgkiladi: record.HESGK_IL_ADI || '',
        hestel: record.HES_TEL || '',
        heseposta: record.HES_EPOSTA || '',
        hesno: record.HES_NO || '',
        doviztip: record.DOVIZ_TIP || '',
        hsptip: record.HSP_TIP || '',
        kisiad: record.KISI_AD || '',
        kisisoyad: record.KISI_SOYAD || '',
        kisikimliktipi: record.KISI_KIMLIK_TIPI || '',
        kisikimlikno: record.KISI_KIMLIK_NO || '',
        istar: record.IS_TAR?.substring(0,8) || '',
        isknl: record.IS_KNL || '',
        bankaad: record.BANKA_AD || '',
        islemtutar: `${record.ISLEM_TUTAR}.00` || '',
        asilparatutar: `${record.ASIL_PARA_TUTAR}.00` || '',
        parabirim: record.PARA_BIRIM || '',
        brutkomtut: record.BRUT_KOM_TUT || '',
        musaciklama: record.MUS_ACIKLAMA || '',
        kuraciklama: record.KUR_ACIKLAMA || '',
        kurumkod: record.KURUM_KOD || '',
      });
    }
 
    await writerEphpycni.close();

    // EK6

    const writerYt = await ParquetWriter.openFile(ytSchema, `/home/deuslu/api/Uat/gib/src/parquetFiles/yt/${ytKurumKod}_YT_${year}_${month}_${day}_0004.parquet`);

    for (const record of ytData) {
      await writerYt.appendRow({
        recordtype: record.RECORD_TYPE || '',
        lref: record.L_REF || '',
        islemturu: record.ISLEM_TURU || '',
        gonmusterimi: record.GON_MUSTERI_MI || '',
        gonokvkn: record.GON_OK_VKN || '',
        gonokunvan: record.GON_OK_UNVAN || '',
        gontkvkn: record.GON_TK_VKN || '',
        gontkunvan: record.GON_TK_UNVAN || '',
        gongkad: record.GON_GK_AD || '',
        gongksoyad: record.GON_GK_SOYAD || '',
        gongkkimliktipi: record.GON_GK_KIMLIK_TIPI || '',
        gongkkimlikno: record.GON_GK_KIMLIK_NO || '',
        gongkuyruk: record.GON_GK_UYRUK || '',
        gonadres: record.GON_ADRES || '',
        gonilceadi: record.GON_ILCE_ADI || '',
        gonpostakod: record.GON_POSTA_KOD || '',
        gonilkod: record.GON_IL_KOD && (record.GON_IL_KOD.length > 1 ? '0' + record.GON_IL_KOD : '00' + record.GON_IL_KOD) || '',
        goniladi: record.GON_IL_ADI || '',
        gontel: record.GON_TEL || '',
        goneposta: record.GON_EPOSTA || '',
        gonokhesno: record.GON_OK_HES_NO || '',
        gonokepara: record.GON_OK_EPARA || '',
        gonokkartno: record.GON_OK_KART_NO || '',
        gonbankaad: record.GON_BANKA_AD || '',
        gonbankakod: record.GON_BANKA_KOD || '',
        gonsubead: record.GON_SUBE_AD || '',
        goniban: record.GON_IBAN || '',
        gonhesno: record.GON_HES_NO || '',
        gonkredikartno: record.GON_KREDI_KART_NO || '',
        gondebitkartno: record.GON_DEBIT_KART_NO || '',
        almusterimi: record.AL_MUSTERI_MI || '',
        alokvkn: record.AL_OK_VKN || '',
        alokunvan: record.AL_OK_UNVAN || '',
        altkvkn: record.AL_TK_VKN || '',
        altkunvan: record.AL_TK_UNVAN || '',
        algkad: record.AL_GK_AD || '',
        algksoyad: record.AL_GK_SOYAD || '',
        algkkimliktipi: record.AL_GK_KIMLIK_TIPI || '',
        algkkimlikno: record.AL_GK_KIMLIK_NO || '',
        algkuyruk: record.AL_GK_UYRUK || '',
        aladres: record.AL_ADRES || '',
        alilceadi: record.AL_ILCE_ADI || '',
        alpostakod: record.AL_POSTA_KOD || '',
        alilkod: record.AL_IL_KOD && (record.AL_IL_KOD.length > 1 ? '0' + record.AL_IL_KOD : '00' + record.AL_IL_KOD) || '',
        aliladi: record.AL_IL_ADI || '',
        altel: record.AL_TEL || '',
        aleposta: record.AL_EPOSTA || '',
        alokhesno: record.AL_OK_HES_NO || '',
        alokepara: record.AL_OK_EPARA || '',
        alokkartno: record.AL_OK_KART_NO || '',
        albankaad: record.AL_BANKA_AD || '',
        albankakod: record.AL_BANKA_KOD || '',
        alsubead: record.AL_SUBE_AD || '',
        aliban: record.AL_IBAN || '',
        alhesno: record.AL_HES_NO || '',
        alkredikartno: record.AL_KREDI_KART_NO || '',
        aldebitkartno: record.AL_DEBIT_KART_NO || '',
        istar: record.IS_TAR || '',
        issaat: record.IS_SAAT || '',
        odenmetar: record.ODENME_TAR?.substring(0,8) || '',
        islemip: record.ISLEM_IP || '',
        islemtutar: record.ISLEM_TUTAR || '',
        asiltutar: record.ASIL_TUTAR || '',
        parabirim: record.PARA_BIRIM || '',
        brutkomtut: record.BRUT_KOM_TUT || '',
        isgonnedeni: record.IS_GON_NEDENI || '',
        islemknl: record.ISLEM_KNL || '',
        subevkn: record.SUBE_VKN || '',
        subeunvan: record.SUBE_UNVAN || '',
        subeiladi: record.SUBE_IL_ADI || '',
        kuraciklama: record.KUR_ACIKLAMA || '',
        musaciklama: record.MUS_ACIKLAMA || '',
        kurumkod: record.KURUM_KOD || ''
      });
    }

    await writerYt.close();

    // EK7

    const writerOkkib = await ParquetWriter.openFile(okkibSchema, `/home/deuslu/api/Uat/gib/src/parquetFiles/okkib/${okkibKurumKod}_OKKIB_${year}_${month}_${day}_0004.parquet`);

    for (const record of okkibData) {
      await writerOkkib.appendRow({
        recordtype: record.RECORD_TYPE || '',
        lref: record.L_REF || '',
        islemturu: record.ISLEM_TURU || '',
        ksahtkvkn: record.K_SAH_TK_VKN || '',
        ksahtkunvan: record.K_SAH_TK_UNVAN || '',
        ksahgkad: record.K_SAH_GK_AD || '',
        ksahgksoyad: record.K_SAH_GK_SOYAD || '',
        ksahgkkimliktipi: record.K_SAH_GK_KIMLIK_TIPI || '',
        ksahgkkimlikno: record.K_SAH_GK_KIMLIK_NO || '',
        ksahkartno: record.K_SAH_KART_NO || '',
        banktip: record.BANK_TIP || '',
        bankeftkod: record.BANK_EFT_KOD || '',
        bankatmkod: record.BANK_ATM_KOD || '',
        istar: record.IS_TAR?.substring(0,8) || '',
        islemtutar: record.ISLEM_TUTAR || '',
        asiltutar: record.ASIL_TUTAR || '',
        parabirim: record.PARA_BIRIM || '',
        brutkomtut: record.BRUT_KOM_TUT || '',
        musaciklama: record.MUS_ACIKLAMA || '',
        kuraciklama: record.KUR_ACIKLAMA || '',
        kurumkod: record.KURUM_KOD || ''
      });
    }

    await writerOkkib.close();
 
    console.log('Parquet files basariyla olusturuldu.');
  } catch (err) {
    console.error('Parquet files olusturulurken hata!: ', err);
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

async function getLatestFile(directoryPath) {

  const files = await fs.promises.readdir(directoryPath);
  const parquetFiles = files.filter(file => file.endsWith('.parquet'));

  let latestFile = null;
  let latestDate = null;
 
  parquetFiles.forEach(file => {
    const match = file.match(/(\d+)_(\d+)_(\d+)_\d+\.parquet/);
    if (match) {
      const [_, year, month, day] = match.map(Number);
      const fileDate = new Date(year, month - 1, day);
      if (!latestDate || fileDate > latestDate) {
        latestDate = fileDate;
        latestFile = file;
      }
    }
  });
 
  return latestFile ? {fullPath: path.join(directoryPath, latestFile), filePath: latestFile} : null;
}

async function connectSftpServer() {

  try {
    console.log('Bağlanti kurulmaya calisiliyor..');
    await sftp.connect(connectionConfig).then(res => {
      console.log('ss');
    }).catch(err => console.log('hata'))

    return sftp;
  } catch (err) {
    return 'Connection Error! => ' + err;
  }
}

async function uploadParquetFiles() {

  let sftp = new Client();

  const connectionConfig = {
    host: '212.174.188.170',
    port: '22',
    username: 'ziraatpay',
    password: 'Kukv41g0kc'
  }

  const filesTypes = ["epkbb", "ephpycni", "yt", "okkib"]

  const localFilesPaths = {
    epkbb: '/home/deuslu/api/Uat/gib/src/parquetFiles/epkbb/',
    ephpycni: '/home/deuslu/api/Uat/gib/src/parquetFiles/ephpycni/',
    yt: '/home/deuslu/api/Uat/gib/src/parquetFiles/yt/',
    okkib: '/home/deuslu/api/Uat/gib/src/parquetFiles/okkib/'
  }

  const remoteDirectoryPath = "/data/test/"

  try {
    await sftp.connect(connectionConfig);
    console.log('Connection successful!');
 
    for (const fileType of filesTypes) {
      const latestFilePath = await getLatestFile(localFilesPaths[fileType]);
 
      if (!latestFilePath) {
        console.log('No valid file found');
        continue;
      }

      const remoteFilePath = `${remoteDirectoryPath}${fileType.toUpperCase()}/${latestFilePath.filePath}`
 
      await sftp.put(latestFilePath?.fullPath, remoteFilePath);
      console.log('File successfully transferred:', latestFilePath);
    }
 
    await sftp.end();
  } catch (err) {
    console.error('Error:', err);
  }
}



module.exports = {
  gibGetEpkbb,
  gibUpdateEpkbb,
  gibGetEphpycni,
  gibUpdateEphpycni,
  gibGetYt,
  gibUpdateYt,
  gibGetOkkib,
  gibUpdateOkkib,
  gibGetMenu,
  gibGetError,
  gibInsertError,
  createParquetFiles,
  uploadParquetFiles
};
