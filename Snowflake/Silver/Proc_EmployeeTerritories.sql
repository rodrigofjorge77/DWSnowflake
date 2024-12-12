USE SCHEMA DW.SILVER;

CREATE OR REPLACE PROCEDURE SILVER_EMPLOYEE_TERRITORIES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
	
    create TABLE IF NOT EXISTS DW.SILVER.EMPLOYEE_TERRITORIES (
        EMPLOYEE_ID NUMBER(38,0) NOT NULL,
        TERRITORY_ID VARCHAR(20) NOT NULL,
        DATA_EXTRACAO DATE
    );

	TRUNCATE TABLE DW.SILVER.EMPLOYEE_TERRITORIES;

     -- Inicia uma transação
    BEGIN TRANSACTION;

    -- Insere os dados transformados na tabela DW.SILVER.CATEGORIES
    INSERT INTO DW.SILVER.EMPLOYEE_TERRITORIES (EMPLOYEE_ID,TERRITORY_ID,DATA_EXTRACAO)    
    SELECT COALESCE(EMPLOYEE_ID,-1), 
        COALESCE(TERRITORY_ID,'NA'), 
        DATA_EXTRACAO
    FROM DW.BRONZE.EMPLOYEE_TERRITORIES;

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

CALL SILVER_EMPLOYEE_TERRITORIES();

