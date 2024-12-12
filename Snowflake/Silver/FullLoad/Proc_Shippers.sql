USE SCHEMA DW.SILVER;

CREATE OR REPLACE PROCEDURE SILVER_SHIPPERS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
	
    create TABLE IF NOT EXISTS DW.SILVER.SHIPPERS (
        SHIPPER_ID NUMBER(38,0) NOT NULL,
        COMPANY_NAME VARCHAR(40) NOT NULL,
        PHONE VARCHAR(24),
        DATA_EXTRACAO DATE
    );

	TRUNCATE TABLE DW.SILVER.SHIPPERS;

     -- Inicia uma transação
    BEGIN TRANSACTION;

    -- Insere os dados transformados na tabela DW.SILVER.CATEGORIES
    INSERT INTO DW.SILVER.SHIPPERS (SHIPPER_ID, COMPANY_NAME, PHONE, DATA_EXTRACAO)       
    SELECT COALESCE(SHIPPER_ID, -1), 
        UPPER(COALESCE(COMPANY_NAME, 'NA')), 
        COALESCE(PHONE,'NA'), 
        DATA_EXTRACAO
    FROM DW.BRONZE.SHIPPERS;

    -- Finaliza a transação
    COMMIT;

    RETURN 'Dados transferidos e transformados com sucesso';

EXCEPTION
    WHEN OTHER THEN
        -- Em caso de erro, realiza o rollback
        ROLLBACK;
        RETURN 'Erro durante a execução da procedure.';    

END;
$$;

CALL SILVER_SHIPPERS();

