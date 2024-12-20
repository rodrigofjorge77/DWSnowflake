USE SCHEMA DW.GOLD;

CREATE OR REPLACE PROCEDURE DW.GOLD.PROC_DIM_PRODUCTS()
RETURNS STRING
LANGUAGE SQL
AS
$$

BEGIN

    create TABLE IF NOT EXISTS DW.GOLD.DIM_PRODUCTS (
        SK_PRODUCT NUMBER AUTOINCREMENT,
        PRODUCT_ID NUMBER(38,0) NOT NULL,
        PRODUCT_NAME VARCHAR(40) NOT NULL,
        SUPPLIER_ID NUMBER(38,0),
        SUPPLIER_NAME VARCHAR(40),
        CATEGORY_ID NUMBER(38,0),
        QUANTITY_PER_UNIT VARCHAR(20),
        UNIT_PRICE FLOAT,
        UNITS_IN_STOCK NUMBER(38,0),
        UNITS_ON_ORDER NUMBER(38,0),
        REORDER_LEVEL NUMBER(38,0),
        DISCONTINUED NUMBER(38,0) NOT NULL,
        DATA_EXTRACAO DATE
    );

    create TABLE if not exists DW.GOLD.CATEGORIES (
    CATEGORY_ID NUMBER(38,0) NOT NULL,
    CATEGORY_NAME VARCHAR(15) NOT NULL,
    DESCRIPTION VARCHAR(16777216),
    DATA_EXTRACAO DATE
    );

    create TABLE if not exists DW.GOLD.SUPPLIERS (
        SUPPLIER_ID NUMBER(38,0) NOT NULL,
        COMPANY_NAME VARCHAR(40) NOT NULL,
        CONTACT_NAME VARCHAR(30),
        CONTACT_TITLE VARCHAR(30),
        ADDRESS VARCHAR(60),
        CITY VARCHAR(15),
        REGION VARCHAR(15),
        POSTAL_CODE VARCHAR(10),
        COUNTRY VARCHAR(15),
        PHONE VARCHAR(24),
        FAX VARCHAR(24),
        HOMEPAGE VARCHAR(16777216),
        DATA_EXTRACAO DATE
    );

    TRUNCATE TABLE DW.GOLD.DIM_PRODUCTS;
    TRUNCATE TABLE DW.GOLD.CATEGORIES;
    TRUNCATE TABLE DW.GOLD.SUPPLIERS;

    -- Inicia uma transação
    BEGIN TRANSACTION;

    INSERT INTO DW.GOLD.DIM_PRODUCTS (PRODUCT_ID, PRODUCT_NAME, SUPPLIER_ID, SUPPLIER_NAME, CATEGORY_ID, QUANTITY_PER_UNIT, UNIT_PRICE, UNITS_IN_STOCK, UNITS_ON_ORDER, REORDER_LEVEL, DISCONTINUED, DATA_EXTRACAO)
    SELECT P.PRODUCT_ID, P.PRODUCT_NAME, P.SUPPLIER_ID, S.COMPANY_NAME,  P.CATEGORY_ID, P.QUANTITY_PER_UNIT, P.UNIT_PRICE, P.UNITS_IN_STOCK, P.UNITS_ON_ORDER, P.REORDER_LEVEL, P.DISCONTINUED, P.DATA_EXTRACAO
    FROM DW.SILVER.PRODUCTS P ,
         DW.SILVER.SUPPLIERS S
    WHERE S.SUPPLIER_ID  = P.SUPPLIER_ID ;


    INSERT INTO DW.GOLD.CATEGORIES (CATEGORY_ID, CATEGORY_NAME, DESCRIPTION, DATA_EXTRACAO)
    SELECT 
        CATEGORY_ID, 
        CATEGORY_NAME,
        DESCRIPTION,
        DATA_EXTRACAO
    FROM DW.SILVER.CATEGORIES;

    INSERT INTO DW.GOLD.SUPPLIERS (SUPPLIER_ID, COMPANY_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION, POSTAL_CODE, COUNTRY, PHONE, FAX, HOMEPAGE, DATA_EXTRACAO)
    SELECT SUPPLIER_ID, COMPANY_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION, POSTAL_CODE, COUNTRY, PHONE, FAX, HOMEPAGE, DATA_EXTRACAO
    FROM DW.SILVER.SUPPLIERS;

    COMMIT;

    RETURN 'Dados inseridos com sucesso na tabela DIM_PRODUCTS!';

EXCEPTION
    WHEN OTHER THEN
        -- Em caso de erro, realiza o rollback
        ROLLBACK;
        RETURN 'Erro durante a execução da procedure.';

END;
$$;

CALL DW.GOLD.PROC_DIM_PRODUCTS();