USE SCHEMA DW.GOLD;

CREATE OR REPLACE PROCEDURE DW.GOLD.PROC_DIM_SHIPPERS()
RETURNS STRING
LANGUAGE SQL
AS
$$

BEGIN

    create TABLE IF NOT EXISTS DW.GOLD.DIM_SHIPPERS (
        SK_SHIPPERS NUMBER AUTOINCREMENT,
        SHIPPER_ID NUMBER(38,0) NOT NULL,
        COMPANY_NAME VARCHAR(40) NOT NULL,
        PHONE VARCHAR(24),
        DATA_EXTRACAO DATE
    );

    TRUNCATE TABLE DW.GOLD.DIM_SHIPPERS;

    -- Inicia uma transação
    BEGIN TRANSACTION;

    INSERT INTO DW.GOLD.DIM_SHIPPERS (SHIPPER_ID, COMPANY_NAME, PHONE, DATA_EXTRACAO)
    SELECT SHIPPER_ID, COMPANY_NAME, PHONE, DATA_EXTRACAO
    FROM DW.SILVER.SHIPPERS;

    COMMIT;

    RETURN 'Dados inseridos com sucesso na tabela DIM_SHIPPERS!';

EXCEPTION
    WHEN OTHER THEN
        -- Em caso de erro, realiza o rollback
        ROLLBACK;
        RETURN 'Erro durante a execução da procedure.';

END;
$$;

CALL DW.GOLD.PROC_DIM_SHIPPERS();