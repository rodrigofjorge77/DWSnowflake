USE SCHEMA DW.SILVER;

CREATE OR REPLACE PROCEDURE SILVER_REGION()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
	
    create TABLE IF NOT EXISTS DW.SILVER.REGION (
        REGION_ID NUMBER(38,0) NOT NULL,
        REGION_DESCRIPTION VARCHAR(60) NOT NULL,
        DATA_EXTRACAO DATE
    );

	TRUNCATE TABLE DW.SILVER.REGION;

     -- Inicia uma transação
    BEGIN TRANSACTION;

    -- Insere os dados transformados na tabela DW.SILVER.CATEGORIES
    INSERT INTO DW.SILVER.REGION (REGION_ID,REGION_DESCRIPTION,DATA_EXTRACAO)       
    SELECT COALESCE(REGION_ID,-1), 
        UPPER(COALESCE(REGION_DESCRIPTION,'NA')), 
        DATA_EXTRACAO
    FROM DW.BRONZE.REGION;

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

CALL SILVER_REGION();

