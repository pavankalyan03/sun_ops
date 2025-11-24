CREATE OR REPLACE PROCEDURE BRONZE.DEV.ECW_NIGHTLY_C_UPDATES_004()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 8,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"transact\",  \"convertedOn\": \"06/09/2025\",  \"domain\": \"s4ch\" }}'
EXECUTE AS CALLER
AS '
	DECLARE


		--% System Setup %--
		START_STEP TIMESTAMP_NTZ(3);
		END_STEP TIMESTAMP_NTZ(3);
		ERRORMSG VARCHAR(8000);
		RECORDS INT;
		TABLE_NAME VARCHAR(255);
		OBJECT_SCHEMA_NAME VARCHAR DEFAULT ''billing'';
		OBJECT_NAME VARCHAR DEFAULT ''ecw_nightly_c_updates_004'';
		OBJ_NAME VARCHAR(255) := :OBJECT_SCHEMA_NAME || ''.'' || :OBJECT_NAME;
		LOAD_DESC VARCHAR(8000);
		STEP_STATUS VARCHAR(8000);
		BATCH_NUMBER VARCHAR(255) := ''Batch.'' || RIGHT(:OBJ_NAME,3);
		BATCH_NUMBER_INT INT := TRY_CAST(RIGHT(:OBJ_NAME,3) AS INT) /*** SSC-FDM-TS0005 - TRY_CONVERT/TRY_CAST COULD NOT BE CONVERTED TO TRY_CAST ***/;
		BATCH_STEP_INT INT := (SELECT CAST(ASCII(SUBSTRING(:OBJ_NAME, 21, 1)) AS INT) - 95);
		RTP_VARIABLE INT := 0;
		OK_TO_MOVE_ON INT;
		RETRIES INT := (SELECT distinct
				MAX(settings_value) FROM (SELECT distinct
						MAX(NVL(TRY_CAST(settings_value AS INT) /*** SSC-FDM-TS0005 - TRY_CONVERT/TRY_CAST COULD NOT BE CONVERTED TO TRY_CAST ***/,0)) settings_value
					FROM HHLI_TRANS.INTERNAL.s4ch_settings
					WHERE
						settings_feature = ''REFRESH_SETTINGS'' and settings_name = ''MAX_RETRIES_ETL'' and settings_active = 1 UNION ALL SELECT 0 settings_value
				) qry
		);
		RETRY_WAIT VARCHAR(50) := (SELECT distinct
				MAX(settings_value) FROM (SELECT distinct
						MAX(NVL(settings_value,''00:00:30'')) settings_value
					FROM HHLI_TRANS.INTERNAL.s4ch_settings
					WHERE
						settings_feature = ''REFRESH_SETTINGS'' and settings_name = ''RETRIES_DELAY_ETL'' and settings_active = 1 UNION ALL SELECT '''' settings_value
				) qry
		);
		------------------------------------------------------------------------------------------------------------------------

		--NOTE: DEPENDANCY EXISTS IN ANOTHER STORED PROCEDURE FOR THIS BATCH #--
		RTP INT := 0;
	BEGIN

		BEGIN
			USE HHLI_TRANS.BILLING;

			-- CREATE OR REPLACE TEMPORARY TABLE T_RTP_TABLE (
			-- 	ready_to_process INT
			-- );

			--OK TO BEGIN PROCESSING?
		-- 	WHILE -- End of loop
		-- 	(RTP_VARIABLE = 0 -- Loop to see if stored procedure is ready execute
		-- 	) LOOP
		-- 	-- End of loop

		-- 		DELETE FROM
		-- 			T_RTP_TABLE;
		-- 		INSERT INTO T_RTP_TABLE
		-- 		SELECT
		-- 			MIN(ready_to_process)
		-- 		FROM (	SELECT case when trgr_current_value is null then 1 else 0 end ready_to_process
		-- 				FROM HHLI_TRANS.INTERNAL.s4ch_load_triggers
		-- 				WHERE
		-- 					trgr_current_value < :BATCH_STEP_INT - 1
		-- 				UNION ALL
		-- 				SELECT 1 ready_to_process
		-- 			) sqry_1;
		-- 		RTP_VARIABLE := (SELECT
		-- 				*
		-- 			FROM
		-- 				T_rtp_table
		-- 		);
		-- 		IF (:RTP_VARIABLE < 1) THEN
		-- 			BEGIN
						
		-- 				CALL SYSTEM$WAIT(60);
		-- 			END;
		-- 		END IF;
		-- 	END LOOP;
			
		-- CALL SYSTEM$PRINT(''BEGIN PROCESSING'');
			
		-- CALL SYSTEM$PRINT(CAST(CURRENT_TIMESTAMP() AS STRING));
		/**************************************************************************************/

		--Trigger Setup Table: Remote (Existing Record)
		UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
		SET
					trgr_current_state = ''Running Phase '' || CAST(:BATCH_STEP_INT AS VARCHAR(10)),
					trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
		
		WHERE
					trgr_batch_id = :BATCH_NUMBER_INT;
			/**************************************************************************************/

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_hx_splitclaims'';
			LOAD_DESC := ''Truncate & Load'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					--% Processing Split Source Info #1 %--
					TRUNCATE TABLE HHLI_TRANS.BILLING.s4ch_hx_splitclaims;
			INSERT INTO HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SELECT	distinct
						Id AS invoice_id,
						NVL(edi_invoice.DeleteFlag,0) AS invoice_delete,
						SplitClaimId AS split_to_01,
					0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0, 0, 0
			FROM {{ CLIENT_PREFIX }}_RAW.BILLING.edi_invoice
			WHERE
						SplitClaimId > Id
						and SplitClaimId > 0 and DeleteFlag = 0
			ORDER BY 3 ASC;
			--% Processing Split Source Info #2 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SET
							split_delete_01 = NVL(deleteFlag,0),
							split_to_02 = case when Id > 0 and SplitClaimId > 0 and SplitClaimId > Id
									then SplitClaimId
								else 0 end
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INVOICE
						WHERE
							s4ch_hx_splitclaims.split_to_01 = edi_invoice.Id;
			--% Processing Split Source Info #3 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SET
							split_delete_02 = NVL(deleteFlag,0),
							split_to_03 = case when Id > 0 and SplitClaimId > 0 and SplitClaimId > Id
									then SplitClaimId
								else 0 end
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INVOICE
						WHERE
							s4ch_hx_splitclaims.split_to_02 = edi_invoice.Id;
			--% Processing Split Source Info #4 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SET
							split_delete_03 = NVL(deleteFlag,0),
							split_to_04 = case when Id > 0 and SplitClaimId > 0 and SplitClaimId > Id
									then SplitClaimId
								else 0 end
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INVOICE
						WHERE
							s4ch_hx_splitclaims.split_to_03 = edi_invoice.Id;
			--% Processing Split Source Info #5 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SET
							split_delete_04 = NVL(deleteFlag,0),
							split_to_05 = case when Id > 0 and SplitClaimId > 0 and SplitClaimId > Id
									then SplitClaimId
								else 0 end
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INVOICE
						WHERE
							s4ch_hx_splitclaims.split_to_04 = edi_invoice.Id;
			--% Processing Split Source Info #6 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SET
							split_delete_05 = NVL(deleteFlag,0),
							split_to_06 = case when Id > 0 and SplitClaimId > 0 and SplitClaimId > Id
									then SplitClaimId
								else 0 end
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INVOICE
						WHERE
							s4ch_hx_splitclaims.split_to_05 = edi_invoice.Id;
			--% Processing Split Source Info #7 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SET
							split_delete_06 = NVL(deleteFlag,0),
							split_to_07 = case when Id > 0 and SplitClaimId > 0 and SplitClaimId > Id
									then SplitClaimId
								else 0 end
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INVOICE
						WHERE
							s4ch_hx_splitclaims.split_to_06 = edi_invoice.Id;
			--% Processing Split Source Info #8 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SET
							split_delete_07 = NVL(deleteFlag,0),
							split_to_08 = case when Id > 0 and SplitClaimId > 0 and SplitClaimId > Id
									then SplitClaimId
								else 0 end
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INVOICE
						WHERE
							s4ch_hx_splitclaims.split_to_07 = edi_invoice.Id;
			--% Processing Split Source Info #9 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SET
							split_delete_08 = NVL(deleteFlag,0),
							split_to_09 = case when Id > 0 and SplitClaimId > 0 and SplitClaimId > Id
									then SplitClaimId
								else 0 end
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INVOICE
						WHERE
							s4ch_hx_splitclaims.split_to_08 = edi_invoice.Id;
			--% Processing Split Source Info #10 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SET
							split_delete_09 = NVL(deleteFlag,0),
							split_to_10 = case when Id > 0 and SplitClaimId > 0 and SplitClaimId > Id
									then SplitClaimId
								else 0 end
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INVOICE
						WHERE
							s4ch_hx_splitclaims.split_to_09 = edi_invoice.Id;
			--% Processing Split Source Info #11 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SET
							split_delete_10 = NVL(deleteFlag,0),
							split_to_11 = case when Id > 0 and SplitClaimId > 0 and SplitClaimId > Id
									then SplitClaimId
								else 0 end
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INVOICE
						WHERE
							s4ch_hx_splitclaims.split_to_10 = edi_invoice.Id;
			--% Processing Split Source Info #12 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SET
							split_delete_11 = NVL(deleteFlag,0),
							split_to_12 = case when Id > 0 and SplitClaimId > 0 and SplitClaimId > Id
									then SplitClaimId
								else 0 end
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INVOICE
						WHERE
							s4ch_hx_splitclaims.split_to_11 = edi_invoice.Id;
			--% Processing Split Source Info #12:  Delete Flag Only %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_splitclaims
			SET
							split_delete_12 = NVL(deleteFlag,0)
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INVOICE
						WHERE
							s4ch_hx_splitclaims.split_to_12 = edi_invoice.Id;

					--% REMOVE CLAIMS w/ INCORRECT PARENT %--
					--% Delete 01 %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					WHERE
						invoice_id in (	SELECT distinct
								split_to_01
											FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
											WHERE
								split_to_01 > 0);
					--% Delete 02 %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					WHERE
						invoice_id in (	SELECT distinct
								split_to_02
											FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
											WHERE
								split_to_02 > 0);
					--% Delete 03 %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					WHERE
						invoice_id in (	SELECT distinct
								split_to_03
											FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
											WHERE
								split_to_03 > 0);
					--% Delete 04 %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					WHERE
						invoice_id in (	SELECT distinct
								split_to_04
											FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
											WHERE
								split_to_04 > 0);
					--% Delete 05 %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					WHERE
						invoice_id in (	SELECT distinct
								split_to_05
											FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
											WHERE
								split_to_05 > 0);
					--% Delete 06 %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					WHERE
						invoice_id in (	SELECT distinct
								split_to_06
											FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
											WHERE
								split_to_06 > 0);
					--% Delete 07 %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					WHERE
						invoice_id in (	SELECT distinct
								split_to_07
											FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
											WHERE
								split_to_07 > 0);
					--% Delete 08 %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					WHERE
						invoice_id in (	SELECT distinct
								split_to_08
											FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
											WHERE
								split_to_08 > 0);
					--% Delete 09 %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					WHERE
						invoice_id in (	SELECT distinct
								split_to_09
											FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
											WHERE
								split_to_09 > 0);
					--% Delete 10 %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					WHERE
						invoice_id in (	SELECT distinct
								split_to_10
											FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
											WHERE
								split_to_10 > 0);
					--% Delete 11 %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					WHERE
						invoice_id in (	SELECT distinct
								split_to_11
											FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
											WHERE
								split_to_11 > 0);
					--% Delete 12 %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					WHERE
						invoice_id in (	SELECT distinct
								split_to_12
											FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
											WHERE
								split_to_12 > 0);
					RECORDS := (SELECT
							COUNT(invoice_id) FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
					);
			UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
			UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
										:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
										MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
										table_name = :TABLE_NAME
										and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
										load_time_end = CURRENT_TIMESTAMP(),
										load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
										) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
										id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
										);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
										trgr_current_state = ''Refresh Failed'',
										trgr_current_value = 0 - :BATCH_STEP_INT,
										trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
										trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--% UPDATING SPLIT CLAIM IDs IN THE INVOICE TABLE %--
			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Split Claim Id(s)'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					--% Update 01 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvSplitId = s4ch_hx_splitclaims.split_to_01
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_splitclaims.invoice_id
							AND (s4ch_hx_splitclaims.split_to_01 > 0);
					--% Update 02 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvSplitId = s4ch_hx_splitclaims.split_to_02
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_splitclaims.invoice_id
							AND (s4ch_hx_splitclaims.split_to_02 > 0);
					--% Update 03 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvSplitId = s4ch_hx_splitclaims.split_to_03
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_splitclaims.invoice_id
							AND (s4ch_hx_splitclaims.split_to_03 > 0);
					--% Update 04 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvSplitId = s4ch_hx_splitclaims.split_to_04
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_splitclaims.invoice_id
							AND (s4ch_hx_splitclaims.split_to_04 > 0);
					--% Update 05 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvSplitId = s4ch_hx_splitclaims.split_to_05
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_splitclaims.invoice_id
							AND (s4ch_hx_splitclaims.split_to_05 > 0);
					--% Update 06 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvSplitId = s4ch_hx_splitclaims.split_to_06
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_splitclaims.invoice_id
							AND (s4ch_hx_splitclaims.split_to_06 > 0);
					--% Update 07 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvSplitId = s4ch_hx_splitclaims.split_to_07
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_splitclaims.invoice_id
							AND (s4ch_hx_splitclaims.split_to_07 > 0);
					--% Update 08 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvSplitId = s4ch_hx_splitclaims.split_to_08
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_splitclaims.invoice_id
							AND (s4ch_hx_splitclaims.split_to_08 > 0);
					--% Update 09 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvSplitId = s4ch_hx_splitclaims.split_to_09
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_splitclaims.invoice_id
							AND (s4ch_hx_splitclaims.split_to_09 > 0);
					--% Update 10 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvSplitId = s4ch_hx_splitclaims.split_to_10
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_splitclaims.invoice_id
							AND (s4ch_hx_splitclaims.split_to_10 > 0);
					--% Update 11 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvSplitId = s4ch_hx_splitclaims.split_to_11
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_splitclaims.invoice_id
							AND (s4ch_hx_splitclaims.split_to_11 > 0);
					--% Update 12 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvSplitId = s4ch_hx_splitclaims.split_to_12
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_splitclaims.invoice_id
							AND (s4ch_hx_splitclaims.split_to_12 > 0);
					RECORDS := 0;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
										:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
										MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
										table_name = :TABLE_NAME
										and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
										load_time_end = CURRENT_TIMESTAMP(),
										load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
										) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
										id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
										);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
										trgr_current_state = ''Refresh Failed'',
										trgr_current_value = 0 - :BATCH_STEP_INT,
										trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
										trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--% UPDATING ORIG ENC IDs IN THE INVOICE TABLE FOR SPLIT CLAIMS %--
			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Original Encounter Id(s) from Split Claim'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					--% Split 01
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.EncounterId
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims split,
							HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvoiceId = split.split_to_01
							AND split.invoice_id = inv2.InvoiceId
							AND (split.split_to_01 > 0);
					RECORDS := NVL(SQLROWCOUNT,0);
					--% Split 02
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.EncounterId
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims split,
							HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvoiceId = split.split_to_02
							AND split.invoice_id = inv2.InvoiceId
							AND (split.split_to_02 > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--% Split 03
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.EncounterId
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims split,
							HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvoiceId = split.split_to_03
							AND split.invoice_id = inv2.InvoiceId
							AND (split.split_to_03 > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--% Split 04
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.EncounterId
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims split,
							HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvoiceId = split.split_to_04
							AND split.invoice_id = inv2.InvoiceId
							AND (split.split_to_04 > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--% Split 05
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.EncounterId
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims split,
							HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvoiceId = split.split_to_05
							AND split.invoice_id = inv2.InvoiceId
							AND (split.split_to_05 > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--% Split 06
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.EncounterId
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims split,
							HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvoiceId = split.split_to_06
							AND split.invoice_id = inv2.InvoiceId
							AND (split.split_to_06 > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--% Split 07
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.EncounterId
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims split,
							HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvoiceId = split.split_to_07
							AND split.invoice_id = inv2.InvoiceId
							AND (split.split_to_07 > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--% Split 08
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.EncounterId
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims split,
							HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvoiceId = split.split_to_08
							AND split.invoice_id = inv2.InvoiceId
							AND (split.split_to_08 > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--% Split 09
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.EncounterId
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims split,
							HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvoiceId = split.split_to_09
							AND split.invoice_id = inv2.InvoiceId
							AND (split.split_to_09 > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--% Split 10
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.EncounterId
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims split,
							HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvoiceId = split.split_to_10
							AND split.invoice_id = inv2.InvoiceId
							AND (split.split_to_10 > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--% Split 11
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.EncounterId
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims split,
							HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvoiceId = split.split_to_11
							AND split.invoice_id = inv2.InvoiceId
							AND (split.split_to_11 > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--% Split 12
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.EncounterId
						FROM HHLI_TRANS.BILLING.s4ch_hx_splitclaims split,
							HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvoiceId = split.split_to_12
							AND split.invoice_id = inv2.InvoiceId
							AND (split.split_to_12 > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
										:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
										MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
										table_name = :TABLE_NAME
										and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
										load_time_end = CURRENT_TIMESTAMP(),
										load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
										) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
										id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
										);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
										trgr_current_state = ''Refresh Failed'',
										trgr_current_value = 0 - :BATCH_STEP_INT,
										trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
										trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--% UPDATING ORIG ENC IDs IN THE INVOICE TABLE FOR SPLIT CLAIMS %--
			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Original Encounter Id(s) from Error(s)'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							OrigEncId = inv2.OrigEncId
						FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv2
						WHERE
							inv.InvSplitId = inv2.InvoiceId
							AND (inv.OrigEncId = inv.EncounterId
							and inv.InvSplitId < inv.InvoiceId);
					RECORDS := NVL(SQLROWCOUNT,0);
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
										:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
										MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
										table_name = :TABLE_NAME
										and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
										load_time_end = CURRENT_TIMESTAMP(),
										load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
										) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
										id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
										);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
										trgr_current_state = ''Refresh Failed'',
										trgr_current_value = 0 - :BATCH_STEP_INT,
										trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
										trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			/**********************************************************************************
			Updating Void-Recreate Claim History
			**********************************************************************************/

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_hx_voidclaims'';
			LOAD_DESC := ''Truncate & Load'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					--% Processing Void-Recreate Source Info #1 %--
					TRUNCATE TABLE HHLI_TRANS.BILLING.s4ch_hx_voidclaims;
			INSERT INTO HHLI_TRANS.BILLING.s4ch_hx_voidclaims
			SELECT	distinct
						InvId AS orig_invoice_id,
						s4ch_view_invoices.OrigEncId AS orig_encounter_id,
						VoidInvId AS void_invoice_id,
						CopyInvId AS new_invoice_id,
					0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
			FROM {{ CLIENT_PREFIX }}_RAW.BILLING.void_claim_log
			INNER JOIN HHLI_TRANS.BILLING.s4ch_view_invoices
			ON void_claim_log.InvId = s4ch_view_invoices.InvoiceId
			LEFT OUTER JOIN (	SELECT	distinct
									CopyInvId AS new_orig_inv
								FROM {{ CLIENT_PREFIX }}_RAW.BILLING.void_claim_log
							) sqry_void_multiple
			ON void_claim_log.InvId = sqry_void_multiple.new_orig_inv
			WHERE
						new_orig_inv is null;
					RECORDS := NVL(SQLROWCOUNT,0);
			--% Processing Void-Recreate Source Info #2 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_voidclaims
			SET
							s4ch_hx_voidclaims.void_invoice_id_02 = void_claim_log.VoidInvId,
							s4ch_hx_voidclaims.new_invoice_id_02 = void_claim_log.CopyInvId
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.void_claim_log
						WHERE
							s4ch_hx_voidclaims.new_invoice_id = void_claim_log.InvId;
			--% Processing Void-Recreate Source Info #3 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_voidclaims
			SET
							s4ch_hx_voidclaims.void_invoice_id_03 = void_claim_log.VoidInvId,
							s4ch_hx_voidclaims.new_invoice_id_03 = void_claim_log.CopyInvId
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.void_claim_log
						WHERE
							s4ch_hx_voidclaims.new_invoice_id_02 = void_claim_log.InvId;
			--% Processing Void-Recreate Source Info #4 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_voidclaims
			SET
							s4ch_hx_voidclaims.void_invoice_id_04 = void_claim_log.VoidInvId,
							s4ch_hx_voidclaims.new_invoice_id_04 = void_claim_log.CopyInvId
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.void_claim_log
						WHERE
							s4ch_hx_voidclaims.new_invoice_id_03 = void_claim_log.InvId;
			--% Processing Void-Recreate Source Info #5 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_voidclaims
			SET
							s4ch_hx_voidclaims.void_invoice_id_05 = void_claim_log.VoidInvId,
							s4ch_hx_voidclaims.new_invoice_id_05 = void_claim_log.CopyInvId
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.void_claim_log
						WHERE
							s4ch_hx_voidclaims.new_invoice_id_04 = void_claim_log.InvId;
			--% Processing Void-Recreate Source Info #6 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_voidclaims
			SET
							s4ch_hx_voidclaims.void_invoice_id_06 = void_claim_log.VoidInvId,
							s4ch_hx_voidclaims.new_invoice_id_06 = void_claim_log.CopyInvId
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.void_claim_log
						WHERE
							s4ch_hx_voidclaims.new_invoice_id_05 = void_claim_log.InvId;
			--% Processing Void-Recreate Source Info #7 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_voidclaims
			SET
							s4ch_hx_voidclaims.void_invoice_id_07 = void_claim_log.VoidInvId,
							s4ch_hx_voidclaims.new_invoice_id_07 = void_claim_log.CopyInvId
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.void_claim_log
						WHERE
							s4ch_hx_voidclaims.new_invoice_id_06 = void_claim_log.InvId;
			--% Processing Void-Recreate Source Info #8 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_voidclaims
			SET
							s4ch_hx_voidclaims.void_invoice_id_08 = void_claim_log.VoidInvId,
							s4ch_hx_voidclaims.new_invoice_id_08 = void_claim_log.CopyInvId
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.void_claim_log
						WHERE
							s4ch_hx_voidclaims.new_invoice_id_07 = void_claim_log.InvId;
			--% Processing Void-Recreate Source Info #9 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_voidclaims
			SET
							s4ch_hx_voidclaims.void_invoice_id_09 = void_claim_log.VoidInvId,
							s4ch_hx_voidclaims.new_invoice_id_09 = void_claim_log.CopyInvId
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.void_claim_log
						WHERE
							s4ch_hx_voidclaims.new_invoice_id_08 = void_claim_log.InvId;
			--% Processing Void-Recreate Source Info #10 %--
			UPDATE HHLI_TRANS.BILLING.s4ch_hx_voidclaims
			SET
							s4ch_hx_voidclaims.void_invoice_id_10 = void_claim_log.VoidInvId,
							s4ch_hx_voidclaims.new_invoice_id_10 = void_claim_log.CopyInvId
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.void_claim_log
						WHERE
							s4ch_hx_voidclaims.new_invoice_id_09 = void_claim_log.InvId;
					RECORDS := 0;
			UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
			UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
										:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
										MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
										table_name = :TABLE_NAME
										and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
										load_time_end = CURRENT_TIMESTAMP(),
										load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
										) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
										id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
										);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
										trgr_current_state = ''Refresh Failed'',
										trgr_current_value = 0 - :BATCH_STEP_INT,
										trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
										trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--% UPDATING ORIG ENC IDs IN THE INVOICE TABLE FOR VOID-RECREATE CLAIMS %--
			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Original Encounter Id(s) from Void-Recreate'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					--% Updating Void-Recreate Source Info #01 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.orig_invoice_id;
					--% Updating Void-Recreate Source Info #02 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.void_invoice_id;
					--% Updating Void-Recreate Source Info #03 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.new_invoice_id;
					--% Updating Void-Recreate Source Info #04 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.void_invoice_id_02;
					--% Updating Void-Recreate Source Info #05 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.new_invoice_id_02;
					--% Updating Void-Recreate Source Info #06 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.void_invoice_id_03;
					--% Updating Void-Recreate Source Info #07 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.new_invoice_id_03;
					--% Updating Void-Recreate Source Info #08 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.void_invoice_id_04;
					--% Updating Void-Recreate Source Info #09 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.new_invoice_id_04;
					--% Updating Void-Recreate Source Info #09 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.void_invoice_id_05;
					--% Updating Void-Recreate Source Info #10 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.new_invoice_id_05;
					--% Updating Void-Recreate Source Info #11 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.void_invoice_id_06;
					--% Updating Void-Recreate Source Info #12 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.new_invoice_id_06;
					--% Updating Void-Recreate Source Info #13 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.void_invoice_id_07;
					--% Updating Void-Recreate Source Info #14 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.new_invoice_id_07;
					--% Updating Void-Recreate Source Info #15 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.void_invoice_id_08;
					--% Updating Void-Recreate Source Info #16 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.new_invoice_id_08;
					--% Updating Void-Recreate Source Info #17 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.void_invoice_id_09;
					--% Updating Void-Recreate Source Info #18 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.new_invoice_id_09;
					--% Updating Void-Recreate Source Info #19 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.void_invoice_id_10;
					--% Updating Void-Recreate Source Info #20 %--
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.OrigEncId = s4ch_hx_voidclaims.orig_encounter_id
						FROM HHLI_TRANS.BILLING.s4ch_hx_voidclaims
						WHERE
							s4ch_view_invoices.InvoiceId = s4ch_hx_voidclaims.new_invoice_id_10;
					RECORDS := 0;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
										:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
										MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
										table_name = :TABLE_NAME
										and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
										load_time_end = CURRENT_TIMESTAMP(),
										load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
										) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
										id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
										);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
										trgr_current_state = ''Refresh Failed'',
										trgr_current_value = 0 - :BATCH_STEP_INT,
										trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
										trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			/****************************************************************************
			Processing General Claim Information
			****************************************************************************/
			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Insurance Information'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					--PRIMARY INSURANCE
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.PriInsSubscriberNo = CAST(REPLACE(REPLACE(REPLACE(UPPER(LTRIM(RTRIM(edi_inv_insurance.subscriberNo))), CHAR(9),''''), CHAR(10),''''), CHAR(13),'''') AS VARCHAR(50)),
							s4ch_view_invoices.PriInsGroupNo = CAST(UPPER(LTRIM(RTRIM(edi_inv_insurance.groupNo))) AS VARCHAR(50))
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INV_INSURANCE
						WHERE
							s4ch_view_invoices.InvoiceId = edi_inv_insurance.InvoiceId
							AND s4ch_view_invoices.InvPriInsId = edi_inv_insurance.InsId
							AND (s4ch_view_invoices.InvPriInsId > 0 AND edi_inv_insurance.deleteFlag = 0);
					RECORDS := NVL(SQLROWCOUNT,0);
					--SECONDARY INSURANCE
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.SecInsSubscriberNo = CAST(REPLACE(REPLACE(REPLACE(UPPER(LTRIM(RTRIM(edi_inv_insurance.subscriberNo))), CHAR(9),''''), CHAR(10),''''), CHAR(13),'''') AS VARCHAR(50)),
							s4ch_view_invoices.SecInsGroupNo = CAST(UPPER(LTRIM(RTRIM(edi_inv_insurance.groupNo))) AS VARCHAR(50))
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INV_INSURANCE
						WHERE
							s4ch_view_invoices.InvoiceId = edi_inv_insurance.InvoiceId
							AND s4ch_view_invoices.InvSecInsId = edi_inv_insurance.InsId
							AND (s4ch_view_invoices.InvSecInsId > 0 AND edi_inv_insurance.deleteFlag = 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--TERTIARY INSURANCE
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.TerInsSubscriberNo = CAST(REPLACE(REPLACE(REPLACE(UPPER(LTRIM(RTRIM(edi_inv_insurance.subscriberNo))), CHAR(9),''''), CHAR(10),''''), CHAR(13),'''') AS VARCHAR(50)),
							s4ch_view_invoices.TerInsGroupNo = CAST(UPPER(LTRIM(RTRIM(edi_inv_insurance.groupNo))) AS VARCHAR(50))
						FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INV_INSURANCE
						WHERE
							s4ch_view_invoices.InvoiceId = edi_inv_insurance.InvoiceId
							AND s4ch_view_invoices.InvTerInsId = edi_inv_insurance.InsId
							AND (s4ch_view_invoices.InvTerInsId > 0 AND edi_inv_insurance.deleteFlag = 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--CURRENT INSURANCE
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices AS s
                    SET 
                        CurInsSubscriberNo = CAST(REPLACE(REPLACE(REPLACE(UPPER(TRIM(edi.subscriberNo)),CHR(9),  '''' ),CHR(10), ''''),CHR(13), '''') AS VARCHAR(50))
						,CurInsGroupNo = CAST(left(UPPER(TRIM(edi.groupNo)),50) AS VARCHAR(50))
                    FROM {{ CLIENT_PREFIX }}_RAW.BILLING.EDI_INV_INSURANCE AS edi
                    WHERE s.InvoiceId  = edi.InvoiceId
                      AND s.InvCurInsId = edi.InsId
                      AND s.InvCurInsId > 0
                      AND edi.deleteFlag = 0;
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
										:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
										MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
										table_name = :TABLE_NAME
										and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
										load_time_end = CURRENT_TIMESTAMP(),
										load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
										) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
										id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
										);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
										trgr_current_state = ''Refresh Failed'',
										trgr_current_value = 0 - :BATCH_STEP_INT,
										trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
										trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Subscriber Id Type(s)'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.subscriber_id_type_pri =
							case when PriInsSubscriberNo > '''' then
							case	when PriInsSubscriberNo like ''[A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][A-Z]'' then ''NYSDOH_CIN''
									when PriInsSubscriberNo like ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][A-Z]'' then ''MCR_HIC''
									when PriInsSubscriberNo like ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][A-Z][A-Z]'' then ''MCR_HIC''
									when PriInsSubscriberNo like ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][A-Z][0-9]'' then ''MCR_HIC''
									when PriInsSubscriberNo like ''[A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' then ''MCR_HIC''
									when PriInsSubscriberNo like ''[A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' then ''MCR_HIC''
									when PriInsSubscriberNo like ''[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' then ''MCR_HIC''
									when LEN(PriInsSubscriberNo) >= 11 and
											(SUBSTRING(PriInsSubscriberNo,1,1) like ''[1-9]'') and
											(SUBSTRING(PriInsSubscriberNo,2,1) like ''[A-Z]'' and SUBSTRING(PriInsSubscriberNo,2,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(PriInsSubscriberNo,3,1) like ''[0-9]'' or
												(SUBSTRING(PriInsSubscriberNo,3,1) like ''[A-Z]'' and SUBSTRING(PriInsSubscriberNo,3,1) not in (''S'',''L'',''O'',''I'',''B'',''Z''))) and
											(SUBSTRING(PriInsSubscriberNo,4,1) like ''[0-9]'') and
											(SUBSTRING(PriInsSubscriberNo,5,1) like ''[A-Z]'' and SUBSTRING(PriInsSubscriberNo,5,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(PriInsSubscriberNo,6,1) like ''[0-9]'' or
												(SUBSTRING(PriInsSubscriberNo,6,1) like ''[A-Z]'' and SUBSTRING(PriInsSubscriberNo,6,1) not in (''S'',''L'',''O'',''I'',''B'',''Z''))) and
											(SUBSTRING(PriInsSubscriberNo,7,1) like ''[0-9]'') and
											(SUBSTRING(PriInsSubscriberNo,8,1) like ''[A-Z]'' and SUBSTRING(PriInsSubscriberNo,8,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(PriInsSubscriberNo,9,1) like ''[A-Z]'' and SUBSTRING(PriInsSubscriberNo,9,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(PriInsSubscriberNo,10,1) like ''[0-9]'') and
											(SUBSTRING(PriInsSubscriberNo,11,1) like ''[0-9]'') then ''MCR_MBI''
							else '''' end else '''' end,
							s4ch_view_invoices.subscriber_id_type_sec =
							case when SecInsSubscriberNo > '''' then
							case	when SecInsSubscriberNo like ''[A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][A-Z]'' then ''NYSDOH_CIN''
									when SecInsSubscriberNo like ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][A-Z]'' then ''MCR_HIC''
									when SecInsSubscriberNo like ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][A-Z][A-Z]'' then ''MCR_HIC''
									when SecInsSubscriberNo like ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][A-Z][0-9]'' then ''MCR_HIC''
									when SecInsSubscriberNo like ''[A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' then ''MCR_HIC''
									when SecInsSubscriberNo like ''[A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' then ''MCR_HIC''
									when SecInsSubscriberNo like ''[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' then ''MCR_HIC''
									when LEN(SecInsSubscriberNo) >= 11 and
											(SUBSTRING(SecInsSubscriberNo,1,1) like ''[1-9]'') and
											(SUBSTRING(SecInsSubscriberNo,2,1) like ''[A-Z]'' and SUBSTRING(SecInsSubscriberNo,2,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(SecInsSubscriberNo,3,1) like ''[0-9]'' or
												(SUBSTRING(SecInsSubscriberNo,3,1) like ''[A-Z]'' and SUBSTRING(SecInsSubscriberNo,3,1) not in (''S'',''L'',''O'',''I'',''B'',''Z''))) and
											(SUBSTRING(SecInsSubscriberNo,4,1) like ''[0-9]'') and
											(SUBSTRING(SecInsSubscriberNo,5,1) like ''[A-Z]'' and SUBSTRING(SecInsSubscriberNo,5,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(SecInsSubscriberNo,6,1) like ''[0-9]'' or
												(SUBSTRING(SecInsSubscriberNo,6,1) like ''[A-Z]'' and SUBSTRING(SecInsSubscriberNo,6,1) not in (''S'',''L'',''O'',''I'',''B'',''Z''))) and
											(SUBSTRING(SecInsSubscriberNo,7,1) like ''[0-9]'') and
											(SUBSTRING(SecInsSubscriberNo,8,1) like ''[A-Z]'' and SUBSTRING(SecInsSubscriberNo,8,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(SecInsSubscriberNo,9,1) like ''[A-Z]'' and SUBSTRING(SecInsSubscriberNo,9,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(SecInsSubscriberNo,10,1) like ''[0-9]'') and
											(SUBSTRING(SecInsSubscriberNo,11,1) like ''[0-9]'') then ''MCR_MBI''
							else '''' end else '''' end,
							s4ch_view_invoices.subscriber_id_type_ter =
							case when TerInsSubscriberNo > '''' then
							case	when TerInsSubscriberNo like ''[A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][A-Z]'' then ''NYSDOH_CIN''
									when TerInsSubscriberNo like ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][A-Z]'' then ''MCR_HIC''
									when TerInsSubscriberNo like ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][A-Z][A-Z]'' then ''MCR_HIC''
									when TerInsSubscriberNo like ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][A-Z][0-9]'' then ''MCR_HIC''
									when TerInsSubscriberNo like ''[A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' then ''MCR_HIC''
									when TerInsSubscriberNo like ''[A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' then ''MCR_HIC''
									when TerInsSubscriberNo like ''[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' then ''MCR_HIC''
									when LEN(TerInsSubscriberNo) >= 11 and
											(SUBSTRING(TerInsSubscriberNo,1,1) like ''[1-9]'') and
											(SUBSTRING(TerInsSubscriberNo,2,1) like ''[A-Z]'' and SUBSTRING(TerInsSubscriberNo,2,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(TerInsSubscriberNo,3,1) like ''[0-9]'' or
												(SUBSTRING(TerInsSubscriberNo,3,1) like ''[A-Z]'' and SUBSTRING(TerInsSubscriberNo,3,1) not in (''S'',''L'',''O'',''I'',''B'',''Z''))) and
											(SUBSTRING(TerInsSubscriberNo,4,1) like ''[0-9]'') and
											(SUBSTRING(TerInsSubscriberNo,5,1) like ''[A-Z]'' and SUBSTRING(TerInsSubscriberNo,5,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(TerInsSubscriberNo,6,1) like ''[0-9]'' or
												(SUBSTRING(TerInsSubscriberNo,6,1) like ''[A-Z]'' and SUBSTRING(TerInsSubscriberNo,6,1) not in (''S'',''L'',''O'',''I'',''B'',''Z''))) and
											(SUBSTRING(TerInsSubscriberNo,7,1) like ''[0-9]'') and
											(SUBSTRING(TerInsSubscriberNo,8,1) like ''[A-Z]'' and SUBSTRING(TerInsSubscriberNo,8,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(TerInsSubscriberNo,9,1) like ''[A-Z]'' and SUBSTRING(TerInsSubscriberNo,9,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(TerInsSubscriberNo,10,1) like ''[0-9]'') and
											(SUBSTRING(TerInsSubscriberNo,11,1) like ''[0-9]'') then ''MCR_MBI''
							else '''' end else '''' end,
							s4ch_view_invoices.subscriber_id_type_cur =
							case when CurInsSubscriberNo > '''' then
							case	when CurInsSubscriberNo like ''[A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][A-Z]'' then ''NYSDOH_CIN''
									when CurInsSubscriberNo like ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][A-Z]'' then ''MCR_HIC''
									when CurInsSubscriberNo like ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][A-Z][A-Z]'' then ''MCR_HIC''
									when CurInsSubscriberNo like ''[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][A-Z][0-9]'' then ''MCR_HIC''
									when CurInsSubscriberNo like ''[A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' then ''MCR_HIC''
									when CurInsSubscriberNo like ''[A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' then ''MCR_HIC''
									when CurInsSubscriberNo like ''[A-Z][A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'' then ''MCR_HIC''
									when LEN(CurInsSubscriberNo) >= 11 and
											(SUBSTRING(CurInsSubscriberNo,1,1) like ''[1-9]'') and
											(SUBSTRING(CurInsSubscriberNo,2,1) like ''[A-Z]'' and SUBSTRING(CurInsSubscriberNo,2,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(CurInsSubscriberNo,3,1) like ''[0-9]'' or
												(SUBSTRING(CurInsSubscriberNo,3,1) like ''[A-Z]'' and SUBSTRING(CurInsSubscriberNo,3,1) not in (''S'',''L'',''O'',''I'',''B'',''Z''))) and
											(SUBSTRING(CurInsSubscriberNo,4,1) like ''[0-9]'') and
											(SUBSTRING(CurInsSubscriberNo,5,1) like ''[A-Z]'' and SUBSTRING(CurInsSubscriberNo,5,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(CurInsSubscriberNo,6,1) like ''[0-9]'' or
												(SUBSTRING(CurInsSubscriberNo,6,1) like ''[A-Z]'' and SUBSTRING(CurInsSubscriberNo,6,1) not in (''S'',''L'',''O'',''I'',''B'',''Z''))) and
											(SUBSTRING(CurInsSubscriberNo,7,1) like ''[0-9]'') and
											(SUBSTRING(CurInsSubscriberNo,8,1) like ''[A-Z]'' and SUBSTRING(CurInsSubscriberNo,8,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(CurInsSubscriberNo,9,1) like ''[A-Z]'' and SUBSTRING(CurInsSubscriberNo,9,1) not in (''S'',''L'',''O'',''I'',''B'',''Z'')) and
											(SUBSTRING(CurInsSubscriberNo,10,1) like ''[0-9]'') and
											(SUBSTRING(CurInsSubscriberNo,11,1) like ''[0-9]'') then ''MCR_MBI''
							else '''' end else '''' end;
					-- FROM HHLI_TRANS.BILLING.s4ch_view_invoices;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Insurance Payment (Pri,Sec,Ter)'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.PriInsPayment = NVL(PriPayment,0) - NVL(PriRefund,0),
							s4ch_view_invoices.SecInsPayment = NVL(SecPayment,0) - NVL(SecRefund,0),
							s4ch_view_invoices.TerInsPayment = NVL(TerPayment,0) - NVL(TerRefund,0),
							s4ch_view_invoices.CurInsPayment = NVL(CurPayment,0) - NVL(CurRefund,0)
						FROM 
							(	SELECT	distinct
									InvInvoiceId,
									SUM(PriPayment) PriPayment,
									SUM(SecPayment) SecPayment,
									SUM(TerPayment) TerPayment,
									SUM(CurPayment) CurPayment
													FROM (	SELECT	distinct
											InvInvoiceId,
											InvDetailId,
																	case when InvPriInsId = HdrPayerId
													then InvPayment
												else 0 end PriPayment,
																	case when InvSecInsId = HdrPayerId
													then InvPayment
												else 0 end SecPayment,
																	case when InvTerInsId = HdrPayerId
													then InvPayment
												else 0 end TerPayment,
																	case when InvCurInsId = HdrPayerId
													then InvPayment
												else 0 end CurPayment
															FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv
															INNER JOIN HHLI_TRANS.BILLING.s4ch_view_invoiceinspmts pmt
												ON inv.InvoiceId = pmt.InvInvoiceId
															WHERE
											InvPayment <> 0) sqry_ins_1
													GROUP BY
									InvInvoiceId
							) sqry_ins_2,
							(	SELECT	distinct
									clm_invoice_id,
									SUM(PriRefund) PriRefund,
									SUM(SecRefund) SecRefund,
									SUM(TerRefund) TerRefund,
									SUM(CurRefund) CurRefund
													FROM (	SELECT	distinct
											clm_invoice_id,
											clm_claim_refund_id,
																	case when InvPriInsId = hdr_payee_id
													then clm_amount
												else 0 end PriRefund,
																	case when InvSecInsId = hdr_payee_id
													then clm_amount
												else 0 end SecRefund,
																	case when InvTerInsId = hdr_payee_id
													then clm_amount
												else 0 end TerRefund,
																	case when InvCurInsId = hdr_payee_id
													then clm_amount
												else 0 end CurRefund
															FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv
															INNER JOIN HHLI_TRANS.BILLING.s4ch_view_invoiceinsrfnds ref
												ON inv.InvoiceId = ref.clm_invoice_id
															WHERE
											clm_amount <> 0) sqry_ref_1
													GROUP BY
									clm_invoice_id
							) sqry_ref_2
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_ins_2.InvInvoiceId(+)
							AND s4ch_view_invoices.InvoiceId = sqry_ref_2.clm_invoice_id(+)
							AND (sqry_ins_2.InvInvoiceId > 0 or sqry_ref_2.clm_invoice_id > 0);
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Calculated Primary Insurance [S4CH Custom Metadata] and Self-Pay Flag'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvCalcPriInsId =	case	when sqry_first_3.HdrPayerId > 0 then sqry_first_3.HdrPayerId
																		when s4ch_view_invoices.InvCurInsId > 0 then s4ch_view_invoices.InvCurInsId
																else NVL(s4ch_view_invoices.InvPriInsId,0) end,
							s4ch_view_invoices.SelfPay = case when s4ch_view_invoices.InvPriInsId = 0 and s4ch_view_invoices.InvSecInsId = 0 and s4ch_view_invoices.InvTerInsId = 0 then 1 else 0 end
						FROM 
							(	SELECT	distinct
									InvInvoiceId,
									HdrPayerId
													FROM (	SELECT	distinct
											HHLI_TRANS.BILLING.s4ch_view_invoiceinspmts.InvInvoiceId,
											s4ch_view_invoiceinspmts.HdrPayerId,
											s4ch_view_invoiceinspmts.InvDetailId,
											RANK() OVER(PARTITION BY
												s4ch_view_invoiceinspmts.InvInvoiceId
											ORDER BY s4ch_view_invoiceinspmts.InvInvoiceId, s4ch_view_invoiceinspmts.InvDetailId ASC) detail_rank
															FROM HHLI_TRANS.BILLING.s4ch_view_invoiceinspmts
															INNER JOIN (	SELECT	distinct
														InvInvoiceId,
														HdrPayerId,
														SUM(InvPayment) AS total_pmt
																			FROM HHLI_TRANS.BILLING.s4ch_view_invoiceinspmts
																			WHERE
														InvPayment <> 0
																			GROUP BY
														InvInvoiceId,
														HdrPayerId
												) sqry_first_1
															ON s4ch_view_invoiceinspmts.InvInvoiceId = sqry_first_1.InvInvoiceId
												and s4ch_view_invoiceinspmts.HdrPayerId = sqry_first_1.HdrPayerId
															WHERE
											s4ch_view_invoiceinspmts.InvPayment > 0 and sqry_first_1.total_pmt > 0) sqry_first_2
													WHERE
									detail_rank = 1) sqry_first_3
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_first_3.InvInvoiceId(+);
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Patient Fee Schedule'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							inv.CalcPatientFeeSchId = NVL(fsl.id, default1.id),
							pt_hx_slide_id = NVL(sqry_feesch_4.fs_id,-1)
						FROM 
                        -- HHLI_TRANS.BILLING.s4ch_view_invoices inv,
							(SELECT distinct
									MAX(id) id
								FROM {{ CLIENT_PREFIX }}_RAW.BILLING.feeschlist
								WHERE
									deleteFlag = 0 and name like ''%slide%f%'' ) default1,
							(	SELECT	distinct
									InvoiceId,
									fs_feesch_id,
									fs_id
													FROM (	SELECT	distinct
											sqry_feesch_2.InvoiceId,
											sqry_feesch_2.fs_id,
											sqry_feesch_2.fs_feesch_id,
											RANK() OVER(PARTITION BY
												sqry_feesch_2.InvoiceId
											ORDER BY sqry_feesch_2.fs_id DESC) feesch_rank
															FROM (	SELECT	distinct
													InvoiceId,
													fs_id,
													fs_feesch_id
																	FROM (	SELECT	distinct
															fs_id,
															fs_patient_id,
															fs_feesch_id,
															fs_assign_dt,
															fs_expire_dt
																			FROM HHLI_TRANS.BILLING.s4ch_view_slidingscales
																			WHERE
															fs_delete_flag = 0 ) sqry_feesch_1
																	INNER JOIN HHLI_TRANS.BILLING.s4ch_view_invoices inv
																	ON sqry_feesch_1.fs_patient_id = inv.PatientId
																	WHERE
													InvServiceDate between fs_assign_dt and fs_expire_dt
											) sqry_feesch_2
									) sqry_feesch_3
													WHERE
									feesch_rank = 1) sqry_feesch_4
								LEFT OUTER JOIN {{ CLIENT_PREFIX }}_RAW.BILLING.feeschlist fsl
								ON sqry_feesch_4.fs_feesch_id = fsl.PtFeeSchId
						WHERE
							1 = 1
							AND inv.InvoiceId = sqry_feesch_4.InvoiceId;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Invoice Diagnoses'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					--PRIMARY ICD
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							inv.PriDxItemId = dx.ItemId
						FROM 
                        -- HHLI_TRANS.BILLING.s4ch_view_invoices inv,
							{{ CLIENT_PREFIX }}_RAW.DIAGNOSIS.edi_inv_diagnosis dx
						WHERE
							inv.InvoiceId = dx.InvoiceId
							AND (dx.icdOrder = 1 and dx.ItemId > 0);
					RECORDS := NVL(SQLROWCOUNT,0);
					--SECONDARY ICD
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							inv.SecDxItemId = dx.ItemId
						FROM 
                        -- HHLI_TRANS.BILLING.s4ch_view_invoices inv,
							{{ CLIENT_PREFIX }}_RAW.DIAGNOSIS.edi_inv_diagnosis dx
						WHERE
							inv.InvoiceId = dx.InvoiceId
							AND (dx.icdOrder = 2 and dx.ItemId > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					--TERTIARY ICD
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							inv.TerDxItemId = dx.ItemId
						FROM 
                        -- HHLI_TRANS.BILLING.s4ch_view_invoices inv,
							{{ CLIENT_PREFIX }}_RAW.DIAGNOSIS.edi_inv_diagnosis dx
						WHERE
							inv.InvoiceId = dx.InvoiceId
							AND (dx.icdOrder = 3 and dx.ItemId > 0);
					RECORDS := :RECORDS + NVL(SQLROWCOUNT,0);
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Encounter Assigned-To Provider'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							enc_assigned_to = enc.AssignedToId
						FROM HHLI_TRANS.ENCOUNTER.S4CH_VIEW_ENCOUNTERS enc
						WHERE
							inv.OrigEncId = enc.EncounterId;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			/****************************************************************************
			Processing Invoice Financials
			****************************************************************************/
			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Financial Adjustments'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvFinancialAdj = NVL(sqry_finadj_1.InvFinancialAdjs,0)
						FROM 
							(	SELECT distinct
									InvId,
									SUM(amount) InvFinancialAdjs
												FROM {{ CLIENT_PREFIX }}_RAW.BILLING.edi_inv_adjustments
												GROUP BY
									InvId
							) sqry_finadj_1
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_finadj_1.InvId;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Insurance Payments and Contractual Adjustments'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvContractAdj = NVL(sqry_inspmt_2.InvContractualAdjs,0),
							s4ch_view_invoices.InvInsurancePmt = NVL(sqry_inspmt_2.InvInsurancePmts,0) - NVL(sqry_insref_2.clm_refunds,0)
						FROM 
							(	SELECT	distinct
									InvInvoiceId,
									SUM(InvAdjustment) InvContractualAdjs,
									SUM(InvPayment) InvInsurancePmts
													FROM HHLI_TRANS.BILLING.s4ch_view_invoiceinspmts
													WHERE (InvPayment <> 0 or InvAdjustment <> 0)
													GROUP BY
									InvInvoiceId
							) sqry_inspmt_2,
							(	SELECT	distinct
									clm_invoice_id,
									SUM(clm_amount) clm_refunds
													FROM HHLI_TRANS.BILLING.s4ch_view_invoiceinsrfnds
													WHERE
									clm_amount <> 0
													GROUP BY
									clm_invoice_id
							) sqry_insref_2
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_inspmt_2.InvInvoiceId(+)
							AND s4ch_view_invoices.InvoiceId = sqry_insref_2.clm_invoice_id(+)
							AND (sqry_inspmt_2.InvInvoiceId > 0 or sqry_insref_2.clm_invoice_id > 0);
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Insurance Refunds'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							inv.InvInsuranceRfnd = Refunds
						FROM 
							(	SELECT	distinct
									clm_invoice_id,
									SUM(clm_amount) Refunds
												FROM HHLI_TRANS.BILLING.s4ch_view_invoiceinsrfnds
												WHERE
									clm_amount <> 0 and clm_invoice_id > 0
												GROUP BY
									clm_invoice_id
							) ref
						WHERE
							inv.InvoiceId = ref.clm_invoice_id;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Patient Payments'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.InvPatientPmt = NVL(InvPatientPmts,0) - NVL(clm_refunds,0)
						FROM 
							(	SELECT	distinct
									InvEncounterId,
									SUM(InvPayment) InvPatientPmts
													FROM HHLI_TRANS.BILLING.s4ch_view_invoicepatpmts
													WHERE
									InvPayment <> 0
													GROUP BY
									InvEncounterId
							) sqry_patpmt_2,
							(	SELECT	distinct
									clm_encounter_id,
									SUM(clm_amount) clm_refunds
													FROM HHLI_TRANS.BILLING.s4ch_view_invoicepatrfnds
													WHERE
									clm_amount <> 0
													GROUP BY
									clm_encounter_id
							) sqry_patref_2
						WHERE
							s4ch_view_invoices.EncounterId = sqry_patpmt_2.InvEncounterId(+)
							AND s4ch_view_invoices.EncounterId = sqry_patref_2.clm_encounter_id(+)
							AND (sqry_patpmt_2.InvEncounterId > 0 or sqry_patref_2.clm_encounter_id > 0);
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Patient Refunds'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							inv.InvPatientRfnd = Refunds
						FROM 
							(	SELECT	distinct
									clm_invoice_id,
									SUM(clm_amount) Refunds
												FROM HHLI_TRANS.BILLING.s4ch_view_invoicepatrfnds
												WHERE
									clm_amount <> 0 and clm_invoice_id > 0
												GROUP BY
									clm_invoice_id
							) ref
						WHERE
							inv.InvoiceId = ref.clm_invoice_id;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Invoice Payment Date(s)'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.payment_date_first = sqry_pmts_1.pmt_dt_first,
							s4ch_view_invoices.payment_date_last = sqry_pmts_1.pmt_dt_last
						FROM 
							(	SELECT	distinct
									InvInvoiceId,
									MIN(case when HdrEOBDate is null then HdrPostedDate
									else HdrEOBDate
									end) AS pmt_dt_first,
									MAX(case when HdrEOBDate is null then HdrPostedDate
									else HdrEOBDate
									end) AS pmt_dt_last
												FROM HHLI_TRANS.BILLING.s4ch_view_invoiceinspmts
												WHERE
									InvPayment > 0 and (HdrEOBDate is not null or HdrPostedDate is not null)
												GROUP BY
									InvInvoiceId
							) sqry_pmts_1
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_pmts_1.InvInvoiceId;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			/****************************************************************************
			Processing Invoice Flags
			****************************************************************************/
			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Qualifying Code Indicator'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.qualifying_code_flag = 1
						FROM 
							(	SELECT	distinct
									s4ch_view_invoicecpts.InvoiceId AS cpt_invoice_id
												FROM HHLI_TRANS.PROCEDURE.s4ch_view_invoicecpts
												INNER JOIN HHLI_TRANS.PROCEDURE.s4ch_spt_cptgroups
												ON s4ch_view_invoicecpts.ItemId = s4ch_spt_cptgroups.ItemId
												WHERE
									s4ch_spt_cptgroups.GroupCode = 100.00) sqry_cpt_1
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_cpt_1.cpt_invoice_id;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			 
		-- WHILE (:RTP <= 0) LOOP
		-- RTP := (SELECT case when MIN(rtp) = 2 then 0 else MIN(rtp) end FROM (SELECT case when load_time_end is not null and load_result not like ''%fail%'' then 1 else 0 end rtp
		-- 					FROM
		-- 						HHLI_TRANS.INTERNAL.s4ch_processingresults
		-- 					WHERE
		-- 						table_name = ''s4ch_view_invoicecpts'' and load_desc = ''Update: Charge Code(s)'' UNION ALL SELECT 2 rtp
		-- 				) qry
		-- );
		-- IF (:RTP = 0) THEN
		-- 			BEGIN
		-- 				!!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR ''WAIT FOR'' NODE ***/!!!
		-- 				WAITFOR DELAY ''00:01''
		-- 			END;
		-- END IF;
		-- END LOOP;
			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Billable Indicator'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.BillableFlag = 1
						FROM 
							(	SELECT
									s4ch_view_invoices.InvoiceId
												FROM HHLI_TRANS.BILLING.s4ch_view_invoices
												INNER JOIN (	SELECT
												HHLI_TRANS.BILLING.s4ch_view_invoices.InvoiceId,
																		case	when s4ch_view_invoices.InvAmount <= 0 then 0
																				when LOWER(s4ch_spt_providers.Name) like ''%non%billable%'' then 0
																				when LOWER(departments.name) like ''%non%billable%'' then 0
																				when LOWER(departments.name) like ''%adhc%'' then 0
																				when sqry_codes_1.InvoiceId is not null then 0
																		else 1 end AS BillableFlag
																FROM HHLI_TRANS.BILLING.s4ch_view_invoices
																LEFT OUTER JOIN HHLI_TRANS.PROVIDER.s4ch_spt_providers
																ON s4ch_view_invoices.ProviderId = s4ch_spt_providers.ProviderID
																LEFT OUTER JOIN {{ CLIENT_PREFIX }}_RAW.ORGANIZATION.departments
																ON s4ch_view_invoices.InvDepartmentId = departments.DeptId
																LEFT OUTER JOIN (	SELECT distinct
															InvoiceId
																					FROM HHLI_TRANS.PROCEDURE.s4ch_view_invoicecpts
																					LEFT OUTER JOIN HHLI_TRANS.PROCEDURE.s4ch_spt_cptgroups
																					ON s4ch_view_invoicecpts.ItemId = s4ch_spt_cptgroups.ItemId
																					WHERE
															ChargeCode = ''99211,A'' or GroupDesc like ''%ehr%ccm%'') sqry_codes_1
																ON s4ch_view_invoices.InvoiceId = sqry_codes_1.InvoiceId
									) sqry_visits_1
												ON s4ch_view_invoices.InvoiceId = sqry_visits_1.InvoiceId
												WHERE
									sqry_visits_1.BillableFlag = 1) sqry_visits_2
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_visits_2.InvoiceId
							AND (exceptions_count = 0);
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Visit Indicator'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.VisitFlag = 1
						FROM 
							(	SELECT
									MIN(InvoiceId) AS min_InvoiceId,
									OrigEncId
												FROM HHLI_TRANS.BILLING.s4ch_view_invoices
												WHERE
									exceptions_count = 0
												GROUP BY
									OrigEncId
							) sqry_visits_3
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_visits_3.min_InvoiceId
							AND (exceptions_count = 0);
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Threshold Visit Indicator'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.ThresholdVisitFlag = 1
						FROM 
							(	SELECT	distinct
									PatientId,
									InvServiceDate,
									MIN(InvoiceId) AS min_invoice_id
												FROM HHLI_TRANS.BILLING.s4ch_view_invoices
												INNER JOIN (	SELECT	distinct
												PatientId AS fb_patient_id,
												InvServiceDate AS fb_service_date,
												MIN(case when InvFirstSubmitDate IS null then TO_TIMESTAMP_NTZ(''12-31-2999'', ''MM-DD-YYYY'')
													else InvFirstSubmitDate
												end) AS fb_date
																FROM HHLI_TRANS.BILLING.s4ch_view_invoices
																INNER JOIN (	SELECT	distinct
															PatientId AS qc_pat_id,
															InvServiceDate AS qc_dos,
															MAX(qualifying_code_flag) AS max_qcf
																				FROM HHLI_TRANS.BILLING.s4ch_view_invoices
																				WHERE
															VisitFlag = 1
																				GROUP BY
															PatientId,
															InvServiceDate
													) sqry_visit_1
																ON s4ch_view_invoices.PatientId = sqry_visit_1.qc_pat_id
													and s4ch_view_invoices.InvServiceDate = sqry_visit_1.qc_dos
													and s4ch_view_invoices.qualifying_code_flag = sqry_visit_1.max_qcf
																WHERE
												VisitFlag = 1
																GROUP BY
												PatientId,
												InvServiceDate
									) sqry_first_billed
												ON s4ch_view_invoices.PatientId = sqry_first_billed.fb_patient_id
									and s4ch_view_invoices.InvServiceDate = sqry_first_billed.fb_service_date
									and
													case when s4ch_view_invoices.InvFirstSubmitDate IS null then TO_TIMESTAMP_NTZ(''12-31-2999'', ''MM-DD-YYYY'')
											else s4ch_view_invoices.InvFirstSubmitDate
									end = sqry_first_billed.fb_date
												WHERE
									VisitFlag = 1
												GROUP BY
									PatientId,
									InvServiceDate
							) sqry_thv_1
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_thv_1.min_invoice_id;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Telemedicine Indicators'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							telemedicine_ind = 1,
							telemedicine_type = telemedicinetype
						FROM 
                        -- HHLI_TRANS.BILLING.s4ch_view_invoices inv,
							(	SELECT	distinct
									InvoiceId,
									MAX(case	when ChargeCode like ''%,V%'' then ''Video''
																		when ChargeCode like ''%,T%'' then ''Telephone''
																		when ChargeCode like ''%,P%'' then ''Patient Portal Conference''
															else ''Unknown'' end) telemedicinetype
													FROM HHLI_TRANS.PROCEDURE.s4ch_view_invoicecpts
													WHERE
									CHARINDEX('','', ChargeCode) = 6 and LEN(ChargeCode) >= 8 and
															(ChargeCode like ''%,V%'' OR ChargeCode like ''%,T%'' OR ChargeCode like ''%,P%'' OR Code like ''D0995'' OR ChargeCode like ''D0999,A'')
													GROUP BY
									InvoiceId
							) cpt
						WHERE
							inv.InvoiceId = cpt.InvoiceId;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Family-Planning Indicator'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.FamilyPlanningFlag = 1
						FROM 
							(	SELECT	distinct
									InvoiceId AS fp_invoice_id
												FROM HHLI_TRANS.BILLING.s4ch_view_invoices
												LEFT OUTER JOIN (	SELECT distinct
												InvoiceId AS cpt_invoice_id
																	FROM HHLI_TRANS.PROCEDURE.s4ch_view_invoicecpts
																	INNER JOIN HHLI_TRANS.PROCEDURE.s4ch_spt_cptgroups
																	ON s4ch_view_invoicecpts.ItemId = s4ch_spt_cptgroups.ItemId
																	WHERE
												GroupCode = -300.00) sqry_cpts_1
												ON s4ch_view_invoices.InvoiceId = sqry_cpts_1.cpt_invoice_id
												LEFT OUTER JOIN (	SELECT distinct
												InvoiceId AS icd_invoice_id
																	FROM {{ CLIENT_PREFIX }}_RAW.DIAGNOSIS.edi_inv_diagnosis
																	INNER JOIN HHLI_TRANS.DIAGNOSIS.s4ch_spt_icdgroups
																	ON edi_inv_diagnosis.ItemId = s4ch_spt_icdgroups.ItemId
																	WHERE
												GroupCode = -700) sqry_icds_1
												ON s4ch_view_invoices.InvoiceId = sqry_icds_1.icd_invoice_id
												WHERE
									sqry_cpts_1.cpt_invoice_id is not null or sqry_icds_1.icd_invoice_id is not null) sqry_fp_1
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_fp_1.fp_invoice_id;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Medicaid Family-Planning Indicator'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.mcd_family_planning = 1
						FROM 
							(	SELECT	distinct
									InvoiceId AS fp_invoice_id
												FROM HHLI_TRANS.BILLING.s4ch_view_invoices
												INNER JOIN (	SELECT distinct
												InvoiceId AS icd_invoice_id
																FROM {{ CLIENT_PREFIX }}_RAW.DIAGNOSIS.edi_inv_diagnosis
																INNER JOIN HHLI_TRANS.DIAGNOSIS.s4ch_spt_icdgroups
																ON edi_inv_diagnosis.ItemId = s4ch_spt_icdgroups.ItemId
																WHERE
												GroupCode = -1600.00) sqry_icds_1
												ON s4ch_view_invoices.InvoiceId = sqry_icds_1.icd_invoice_id
							) sqry_fp_1
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_fp_1.fp_invoice_id;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Chronic Care Management (CCM) Indicator'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.ccm_indicator = 1
						FROM 
							(	SELECT distinct
									InvoiceId AS ccm_invoice_id
												FROM HHLI_TRANS.PROCEDURE.s4ch_view_invoicecpts
												INNER JOIN (SELECT
												*
											FROM HHLI_TRANS.PROCEDURE.s4ch_spt_cptgroups
											WHERE
												GroupCode = -4001.00 ) s4ch_spt_cptgroups
												ON s4ch_view_invoicecpts.ItemId = s4ch_spt_cptgroups.ItemId
							) sqry_fp_1
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_fp_1.ccm_invoice_id;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Chronic Care Management (CoCM) Indicator'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.cocm_indicator = 1
						FROM 
							(	SELECT distinct
									InvoiceId AS ccm_invoice_id
												FROM HHLI_TRANS.PROCEDURE.s4ch_view_invoicecpts
												INNER JOIN (SELECT
												*
											FROM HHLI_TRANS.PROCEDURE.s4ch_spt_cptgroups
											WHERE
												GroupCode = -4002.00 ) s4ch_spt_cptgroups
												ON s4ch_view_invoicecpts.ItemId = s4ch_spt_cptgroups.ItemId
							) sqry_fp_1
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_fp_1.ccm_invoice_id;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Patient Age at Time-of-Service'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							pt_age_on_service_dt =	case	when DATE_PART(month, DOB_DateTime :: TIMESTAMP) > DATE_PART(month, InvServiceDate :: TIMESTAMP)
									then DATEDIFF(year, DOB_DateTime, InvServiceDate) - 1
														when DATE_PART(month, DOB_DateTime :: TIMESTAMP) = DATE_PART(month, InvServiceDate :: TIMESTAMP)
								and DATE_PART(day, DOB_DateTime :: TIMESTAMP) > DATE_PART(day, InvServiceDate :: TIMESTAMP)
									then DATEDIFF(year, DOB_DateTime, InvServiceDate) - 1
												else DATEDIFF(year, DOB_DateTime, InvServiceDate)
							end
						FROM 
                        -- HHLI_TRANS.BILLING.s4ch_view_invoices inv,
							HHLI_TRANS.PATIENT.S4CH_SPT_PATIENTS pat
						WHERE
							inv.PatientId = pat.PID
							AND (InvServiceDate is not null and DOB_DateTime is not null);
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			/***** Separate Valid Claims from Invalid Claims *****/

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoiceexceptions'';
			LOAD_DESC := ''Truncate & Load'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					--% Copy Invalid Data to Separate Table %--
					TRUNCATE TABLE HHLI_TRANS.BILLING.s4ch_view_invoiceexceptions;
			INSERT INTO HHLI_TRANS.BILLING.s4ch_view_invoiceexceptions
			SELECT
						InvoiceId,
						InvSplitId,
						EncounterId,
						OrigEncId,
						PatientId,
						GuarantorId,
						InvServiceDate,
						InvCreateDate,
						InvModifyDate,
						InvFirstSubmitDate,
						InvLastSubmitDate,
						InvTransferDate,
						InvFacilityId,
						InvDepartmentId,
						ProviderId,
						SupervisingProvId,
						ResourceProvId,
						ReferringProvId,
						RenderingProvId,
						PayToProvId,
						InvPriInsId,
						PriInsSubscriberNo,
						PriInsGroupNo,
						InvSecInsId,
						SecInsSubscriberNo,
						SecInsGroupNo,
						InvTerInsId,
						TerInsSubscriberNo,
						TerInsGroupNo,
						InvCurInsId,
						CurInsSubscriberNo,
						CurInsGroupNo,
						InvCalcPriInsId,
						SelfPay,
						InvFeeScheduleId,
						InvAmount,
						InvFinancialAdj,
						InvContractAdj,
						InvInsurancePmt,
						InvPatientPmt,
						InvPtCopay,
						InvPtBalance,
						InvBalance,
						InvStatusCode,
						InvStatusDesc,
						InvPOS,
						InvBillType,
						InvFG37A,
						InvDelayReason,
						PriDxItemId,
						SecDxItemId,
						TerDxItemId,
						InvRespPartyId,
						InvRespPartyRel,
						InvStatementMsg,
						DenialCode1,
						DenialCode2,
						DenialCode3,
						VisitFlag,
						BillableFlag,
						ThresholdVisitFlag,
						FamilyPlanningFlag,
						CalcPatientFeeSchId,
						qualifying_code_flag,
						ext_era_icn_number,
						ext_era_cas_1,
						ext_era_cas_2,
						ext_era_cas_3,
						payment_date_first,
						payment_date_last,
						subscriber_id_type_pri,
						subscriber_id_type_sec,
						subscriber_id_type_ter,
						subscriber_id_type_cur,
						era_remark_code_1,
						era_remark_code_2,
						era_remark_code_3,
						ext_era_remark_1,
						ext_era_remark_2,
						ext_era_remark_3,
						exceptions_deleted,
						exceptions_voided,
						exceptions_cpt_count,
						exceptions_count
			FROM HHLI_TRANS.BILLING.s4ch_view_invoices
			WHERE
						exceptions_count > 0;
					RECORDS := SQLROWCOUNT;
			UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
			UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Delete: Exceptions/Invalid Data'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					--% Remove Invalid Data from Master Table %--
					DELETE FROM HHLI_TRANS.BILLING.s4ch_view_invoices
					WHERE
						exceptions_count > 0;
					RECORDS := SQLROWCOUNT;
			UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
			UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			/****************************************************************************
			Updating Capitation Information
			****************************************************************************/
			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Capitation Indicator and Amount'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices
					SET
							s4ch_view_invoices.cap_adjstmnt_ind = 1,
							s4ch_view_invoices.cap_adjstmnt_amount = sqry_2.adj_amount
						FROM 
							(	SELECT	distinct
									adj_invoice_id,
									adj_amount
												FROM (	SELECT	distinct
											adj_invoice_id,
											SUM(adj_log_amount) adj_amount
														FROM HHLI_TRANS.BILLING.s4ch_view_invoicefinadjs
														WHERE
											LOWER(adj_code_description) like ''%capit%''
														GROUP BY
											adj_invoice_id
									) sqry_1
												WHERE
									adj_amount <> 0) sqry_2
						WHERE
							s4ch_view_invoices.InvoiceId = sqry_2.adj_invoice_id;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			/**********************************************************************************
			Processing MCVR Data File(s)
			**********************************************************************************/
			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_hx_mcvrs'';
			LOAD_DESC := ''Truncate & Load (External File)'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					TRUNCATE TABLE HHLI_TRANS.BILLING.s4ch_hx_mcvrs;
			INSERT INTO HHLI_TRANS.BILLING.s4ch_hx_mcvrs
			SELECT distinct
						mcvr_mc_claim_id,
						mcvr_split_claim_id, NULL, NULL, NULL, 0, NULL, NULL,
						DATE_PART(year, mcvr_dos :: TIMESTAMP),
						mcvr_dos,
						mcvr_pt_mrn
			FROM HHLI_TRANS.BILLING.ext_data_mcvr_claims;
					RECORDS := SQLROWCOUNT;
			UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
			UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------




		/**********************************************************************************
			Processing MCVR Data File(s)
			**********************************************************************************/
			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Roster Payer Information'';
			STEP_STATUS := ''Processing'';
	--INITIALIZE PROCESSING LOGS--
	INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			SYSTEM$LOG_INFO((LEFT(TO_VARCHAR(CURRENT_TIMESTAMP(), ''yyyy-mm-dd hh:mm:ss:ff3''), 255) || '' - ['' || :TABLE_NAME || '']: '' || :LOAD_DESC));
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
				BEGIN
					--CREATING TEMPORARY TABLE(s)
					--% DROP/CREATE TABLE: [tmp_rosterhx_inv] %--
					IF ((SELECT
							COUNT(TABLE_NAME ) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ''tmp_rosterhx_inv'') > 0) THEN
						DROP TABLE IF EXISTS tmp_rosterhx_inv;
					END IF;
					CREATE OR REPLACE TABLE tmp_rosterhx_inv (
						rhx_id BIGINT IDENTITY(1,1) ORDER,
						ehr_patient_id BIGINT,
						roster_month TIMESTAMP_NTZ(3),
						roster_info VARCHAR(8000)
					);
		--Load Temporary Table(s)--
		INSERT INTO tmp_rosterhx_inv
		(ehr_patient_id, roster_month, roster_info)
		SELECT	distinct
						EHR_PatientId,
						RosterMonth,
						roster_info
		FROM (	SELECT
    DISTINCT rost1.EHR_PatientId,
    rost1.RosterMonth,
    NVL(
        (
            SELECT LISTAGG(payer_info, '' | '') WITHIN GROUP (ORDER BY payer_info)
            FROM (
                SELECT DISTINCT
                    EHR_PatientId,
                    RosterMonth,
                    PayorName || 
                    CASE 
                        WHEN MemberLOB IS NOT NULL AND TRIM(MemberLOB) <> '''' 
                             THEN '' ['' || MemberLOB || '']'' 
                        ELSE '''' 
                    END AS payer_info
                FROM (
                    SELECT
                        EHR_PatientId,
                        TO_CHAR(DATE_TRUNC(''MONTH'', RosterMonth), ''YYYY-MM-DD 00:00:00'') AS RosterMonth,
                        PayorName,
                        MAX(
                            CASE 
                                WHEN MemberLOB IS NOT NULL AND LEFT(MemberLOB, 3) = ''UNK'' 
                                    THEN REPLACE(REPLACE(MemberLOB, ''UNK ('', ''''), '')'', '''')
                                ELSE MemberLOB
                            END
                        ) AS MemberLOB
                    FROM ExtData_InsuranceRosters
                    WHERE EHR_PatientId > 0 AND PayorName IS NOT NULL AND TRIM(PayorName) <> ''''
                    GROUP BY EHR_PatientId, TO_CHAR(DATE_TRUNC(''MONTH'', RosterMonth), ''YYYY-MM-DD 00:00:00''), PayorName
                ) concat1
            ) concat2
            WHERE concat2.EHR_PatientId = rost1.EHR_PatientId
              AND concat2.RosterMonth = rost1.RosterMonth
        ),
        ''none''
    ) AS roster_info
FROM (
    SELECT DISTINCT
        EHR_PatientId,
		TO_CHAR(DATE_TRUNC(''MONTH'', RosterMonth), ''YYYY-MM-DD 00:00:00'') AS RosterMonth
    FROM ExtData_InsuranceRosters
    WHERE EHR_PatientId > 0 AND PayorName IS NOT NULL AND TRIM(PayorName) <> ''''
) rost1
GROUP BY rost1.EHR_PatientId, rost1.RosterMonth ) rost2
		ORDER BY EHR_PatientId, RosterMonth ASC;
		--Update Primary Table--
		UPDATE s4ch_view_invoices inv1
		SET
							payer_roster_info = rost1.roster_info
						FROM
							-- s4ch_view_invoices inv1,
							tmp_rosterhx_inv rost1
						WHERE
							inv1.PatientId = rost1.ehr_patient_id
							-- AND TRY_CAST(TO_VARCHAR(CAST(TO_TIMESTAMP_NTZ(inv1.InvServiceDate) AS VARCHAR(8)) || ''01'') AS TIMESTAMP_NTZ(3)) = rost1.roster_month;
							AND TO_CHAR(DATE_TRUNC(''MONTH'', inv1.InvServiceDate), ''YYYY-MM-DD 00:00:00'') = rost1.roster_month;
					RECORDS := SQLROWCOUNT;
			UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
			UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			------------------------------------------------------------------------------------------------------------------------
			/**** END OF MOVED QUERIES ****/
			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: Week-of-Service'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							week_of_service = sqry_2.service_week
						FROM 
                        -- HHLI_TRANS.BILLING.s4ch_view_invoices inv,
							(	SELECT	distinct
									InvServiceDate service_date,
															''Week Ending: '' || CAST(left(TO_TIMESTAMP_NTZ(DATEADD(day, 7- day_of_week, InvServiceDate)),10) AS VARCHAR(10)) service_week
													FROM (	SELECT	distinct
											InvServiceDate,
											DATE_PART(dayofweek, InvServiceDate :: TIMESTAMP) day_of_week
															FROM HHLI_TRANS.BILLING.s4ch_view_Invoices
															WHERE
											InvServiceDate is not null) sqry_1
							) sqry_2
						WHERE
							inv.InvServiceDate = sqry_2.service_date;
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: UDS Exclusion, COVID-Only Visits'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							uds_exclude = 1
					-- FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv
					WHERE
							OrigEncId in (	SELECT	distinct
									EncounterId
											FROM (	SELECT	distinct
											EncounterId
													FROM HHLI_TRANS.ENCOUNTER.s4ch_view_encounters enc
													WHERE
											DepartmentId in (	SELECT distinct
													DeptId
												FROM {{ CLIENT_PREFIX }}_RAW.ORGANIZATION.departments dept
																			WHERE
													dept.name like ''%covid%only%'')
													UNION ALL
													SELECT	distinct
											OrigEncId EncounterId
													FROM (	SELECT	distinct
													OrigEncId,
													MAX(case when grp.ItemId is null then 1 else 0 end) non_covid_code
															FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv
															INNER JOIN HHLI_TRANS.PROCEDURE.s4ch_view_invoicecpts cpt
														ON inv.InvoiceId = cpt.InvoiceId
															LEFT OUTER JOIN (SELECT distinct
																ItemId
															FROM HHLI_TRANS.PROCEDURE.s4ch_spt_cptgroups
															WHERE
																GroupCode in (-2001.00) ) grp
														ON cpt.ItemId = grp.ItemId
															GROUP BY
													OrigEncId
											) qry
													WHERE
											non_covid_code = 0
													UNION ALL
													SELECT	distinct
											order_encounter_id EncounterId
													FROM (	SELECT	distinct
													order_encounter_id
															FROM HHLI_TRANS.CLINICAL_RESULT.s4ch_view_laborders lab
															INNER JOIN
														{{ CLIENT_PREFIX }}_RAW.INTERNAL.ITEMS itm
														ON lab.order_item_id = itm.itemID
															INNER JOIN HHLI_TRANS.BILLING.s4ch_view_invoices inv
														ON lab.order_encounter_id = inv.OrigEncId
															WHERE
													itm.itemName like ''%covid%'' or itm.itemName like ''%corona%virus%'' or itm.itemName like ''%sars%cov%'' ) lab
													LEFT OUTER JOIN (	SELECT distinct
														OrigEncId
																		FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv
																		INNER JOIN HHLI_TRANS.PROCEDURE.s4ch_view_invoicecpts cpt
															ON inv.InvoiceId = cpt.InvoiceId
																		INNER JOIN HHLI_TRANS.PROVIDER.s4ch_spt_providers pro
															ON inv.ResourceProvId = pro.ProviderID
																		WHERE
														cpt.Code between ''99201'' and ''99209'' or cpt.Code between ''99211'' and ''99219'' or cpt.Code between ''99381'' and ''99389'' or cpt.Code between ''99391'' and ''99399'' ) valid_invs
													ON lab.order_encounter_id = valid_invs.OrigEncId
													WHERE
											valid_invs.OrigEncId is null ) qry
							);
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: UDS Exclusion, COVID Follow-Up Visits'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							uds_exclude = 1
					-- FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv
					WHERE
							OrigEncId in (	SELECT	distinct
									EID2
											FROM (	SELECT	distinct
											EID2,
											MAX(CASE WHEN dx.Code not in (''U07.1'',''Z11.59'',''Z03.818'',''Z71.2'',''Z71.89'',''Z71.9'',''Z09'',''Z20.828'') then 1 else 0 end) other_dx
													FROM (	SELECT	distinct
													*,
													RANK() OVER(PARTITION BY
														Patientid, DOS1
													ORDER BY DOS2 ASC) visit_rank
															FROM (	SELECT	distinct
															inv.EncounterId EID1,
															inv.PatientId,
															inv.ServiceDate DOS1,
															inv2.InvServiceDate DOS2,
															inv2.OrigEncId EID2,
															inv2.InvoiceId IID2
																	FROM (	SELECT	distinct
																	PatientId,
																	ServiceDate,
																	EncounterId
																			FROM (	SELECT	distinct
																			PatientId,
																			ServiceDate,
																			EncounterId
																					FROM HHLI_TRANS.ENCOUNTER.s4ch_view_encounters enc
																					WHERE
																			DepartmentId in (	SELECT distinct
																					DeptId
																				FROM {{ CLIENT_PREFIX }}_RAW.ORGANIZATION.departments dept
																											WHERE
																					dept.name like ''%covid%only%'')
																					UNION ALL
																					SELECT	distinct
																			PatientId,
																			ServiceDate,
																			EncounterId
																					FROM (	SELECT	distinct
																					order_patient_id PatientId,
																					InvServiceDate ServiceDate,
																					order_encounter_id EncounterId
																							FROM HHLI_TRANS.CLINICAL_RESULT.s4ch_view_laborders lab
																							INNER JOIN
																									{{ CLIENT_PREFIX }}_RAW.INTERNAL.ITEMS itm
																									ON lab.order_item_id = itm.itemID
																							INNER JOIN HHLI_TRANS.BILLING.s4ch_view_invoices inv
																									ON lab.order_encounter_id = inv.OrigEncId
																							WHERE
																					itm.itemName like ''%covid%'' or itm.itemName like ''%corona%virus%'' or itm.itemName like ''%sars%cov%'' ) lab
																					LEFT OUTER JOIN (	SELECT distinct
																									OrigEncId
																										FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv
																										INNER JOIN HHLI_TRANS.PROCEDURE.s4ch_view_invoicecpts cpt
																										ON inv.InvoiceId = cpt.InvoiceId
																										INNER JOIN HHLI_TRANS.PROVIDER.s4ch_spt_providers pro
																										ON inv.ResourceProvId = pro.ProviderID
																										WHERE
																									cpt.Code between ''99201'' and ''99209'' or cpt.Code between ''99211'' and ''99219'' or cpt.Code between ''99381'' and ''99389'' or cpt.Code between ''99391'' and ''99399'' ) valid_invs
																					ON lab.EncounterId = valid_invs.OrigEncId
																					WHERE
																			valid_invs.OrigEncId is null ) qry
															) inv
																	INNER JOIN HHLI_TRANS.BILLING.s4ch_view_invoices inv2
																ON inv.PatientId = inv2.PatientId
																	WHERE
															inv2.InvServiceDate > inv.ServiceDate
													) sqry_1
											) sqry_2
													INNER JOIN {{ CLIENT_PREFIX }}_RAW.DIAGNOSIS.edi_inv_diagnosis dx
												ON sqry_2.IID2 = dx.InvoiceId
													WHERE
											visit_rank = 1
													GROUP BY
											EID2
									) sqry_3
											WHERE
									other_dx = 0 );
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: UDS Exclusion, Vaccine-Only Visits'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							uds_exclude = 1
					-- FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv
					WHERE
							InvoiceId in (	SELECT	distinct
									InvoiceId
											FROM (	SELECT	distinct
											inv.InvoiceId,
											COUNT(cpt.InvoiceCodeId) total_cpts,
											SUM(	case	when cpt.Code like ''906%'' then 1
																			when cpt.Code like ''907%'' and cpt.Code not in (''90785'',''90791'',''90792'') then 1
																			when cpt.Code between ''90471'' and ''90474'' then 1
																			when cpt.Code between ''90460'' and ''90461'' then 1
																			when cpt.Code between ''91300'' and ''91303'' then 1
																			when cpt.Code between ''G0008'' and ''G0010'' then 1
																			when cptg.ItemId > 0 then 1
																	else 0 end) vaccine_cpts
													FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv
													INNER JOIN HHLI_TRANS.PROCEDURE.s4ch_view_invoicecpts cpt
												ON inv.InvoiceId = cpt.InvoiceId
													LEFT OUTER JOIN (SELECT distinct
														ItemId
													FROM HHLI_TRANS.PROCEDURE.s4ch_spt_cptgroups
													WHERE
														GroupCode = -2001.00) cptg
												ON cpt.ItemId = cptg.ItemId
													GROUP BY
											inv.InvoiceId
									) inv1
											WHERE
									total_cpts = vaccine_cpts
							);
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: UDS Exclusion, CCM/CoCM-Only Visits'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							uds_exclude = 1
					-- FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv
					WHERE
							InvoiceId in (	SELECT	distinct
									InvoiceId
											FROM (	SELECT	distinct
											inv.InvoiceId,
											COUNT(cpt.InvoiceCodeId) total_cpts,
											SUM(case when cptg.ItemId is not null then 1 else 0 end) ccm_cocm_cpts
													FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv
													INNER JOIN HHLI_TRANS.PROCEDURE.s4ch_view_invoicecpts cpt
												ON inv.InvoiceId = cpt.InvoiceId
													LEFT OUTER JOIN (SELECT distinct
														ItemId
													FROM HHLI_TRANS.PROCEDURE.s4ch_spt_cptgroups
													WHERE
														GroupCode in (-4001.00,-4002.00) ) cptg
												ON cpt.ItemId = cptg.ItemId
													GROUP BY
											inv.InvoiceId
									) inv1
											WHERE
									total_cpts = ccm_cocm_cpts
							);
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
			------------------------------------------------------------------------------------------------------------------------

			--SET INITIAL VARIABLES--
			TABLE_NAME := ''s4ch_view_invoices'';
			LOAD_DESC := ''Update: UDS Exclusion, Non-FQHC Site(s)'';
			STEP_STATUS := ''Processing'';
		--INITIALIZE PROCESSING LOGS--
		INSERT INTO HHLI_TRANS.INTERNAL.s4ch_processingresults (table_name, obj_name, load_desc, load_time_begin) VALUES (:TABLE_NAME, :OBJ_NAME, :LOAD_DESC, CURRENT_TIMESTAMP() :: TIMESTAMP);
			-- CALL SYSTEM$PRINT(TO_VARCHAR(CURRENT_TIMESTAMP) || '' - ['' || table_name || '']: '' || load_desc);
			--BEGIN WORK
			OK_TO_MOVE_ON := :RETRIES;
			WHILE (:OK_TO_MOVE_ON > -1) LOOP
		BEGIN
					UPDATE HHLI_TRANS.BILLING.s4ch_view_invoices inv
					SET
							uds_exclude = 1
					-- FROM HHLI_TRANS.BILLING.s4ch_view_invoices inv
					WHERE
							InvFacilityId in (	SELECT distinct
									fac.Id
												FROM {{ CLIENT_PREFIX }}_RAW.ORGANIZATION.edi_facilities fac
												INNER JOIN (SELECT distinct ''|'' || settings_value || ''|'' settings_value
											FROM HHLI_TRANS.INTERNAL.s4ch_settings
											WHERE
												settings_feature = ''UDS_SETTINGS'' and settings_name = ''EXCLUSION_SITES'' and settings_active = 1 ) udsexc
												ON udsexc.settings_value like ''%'' || ''|'' || CAST(fac.Id AS VARCHAR(50)) || ''|'' || ''%''
												UNION ALL
												SELECT distinct -1 Id
							);
					RECORDS := SQLROWCOUNT;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
						set
							load_time_end = CURRENT_TIMESTAMP(),
							load_result = LEFT(''Completed [Retried '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
							) AS VARCHAR(2)) || '' Times]'' || case when :OK_TO_MOVE_ON = :RETRIES
									then '''' else '': '' || NVL(:ERRORMSG, '''') end,255),
							load_record_count = :RECORDS
						WHERE
							id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
								WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
							);
					OK_TO_MOVE_ON := 0;
		EXCEPTION
					WHEN OTHER THEN
						RECORDS := 0;
						ERRORMSG := SQLERRM;
					UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
							set
								load_result = LEFT(''Failed [Retry # '' || cast((SELECT
									:RETRIES - :OK_TO_MOVE_ON
								) AS VARCHAR(2)) || '']: '' || NVL(:ERRORMSG, ''''),255) WHERE
								id = (SELECT
									MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
									WHERE
									table_name = :TABLE_NAME
									and load_desc = :LOAD_DESC
								);
						IF (:OK_TO_MOVE_ON = 0) THEN
							BEGIN
								UPDATE HHLI_TRANS.INTERNAL.s4ch_processingresults
									set
									load_time_end = CURRENT_TIMESTAMP(),
									load_result = LEFT(''Failed [Retried '' || cast((SELECT
												:RETRIES - :OK_TO_MOVE_ON
									) AS VARCHAR(2)) || '' Times]: '' || NVL(:ERRORMSG, ''''),255) WHERE
									id = (SELECT
												MAX(id) FROM HHLI_TRANS.INTERNAL.s4ch_processingresults
											WHERE
												table_name = :TABLE_NAME
												and load_desc = :LOAD_DESC
									);
								UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
								SET
									trgr_current_state = ''Refresh Failed'',
									trgr_current_value = 0 - :BATCH_STEP_INT,
									trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
								
								WHERE
									trgr_batch_id = :BATCH_NUMBER_INT;
								-- CALL SYSTEM$PRINT(''Waiting for RETRY_WAIT = '' || RETRY_WAIT);
							END;
						END IF;
		END;
		OK_TO_MOVE_ON := :OK_TO_MOVE_ON - 1;
			END LOOP;
		------------------------------------------------------------------------------------------------------------------------


		--UPDATE eCW LOAD TRIGGERS
		UPDATE HHLI_TRANS.INTERNAL.s4ch_load_triggers
		SET
					trgr_current_state = ''Phase '' || CAST(:BATCH_STEP_INT AS VARCHAR(2)) || '' Completed'',
					trgr_current_value = :BATCH_STEP_INT,
					trgr_updated_at = CURRENT_TIMESTAMP() :: TIMESTAMP
		
		WHERE
					trgr_batch_id = :BATCH_NUMBER_INT
					and trgr_current_state not like ''%Fail%'';
		END;

--		/****** Object:  StoredProcedure dbo.[ecw_nightly_c_updates_005]    Script Date: 4/17/2024 1:26:46 PM ******/
--		--** SSC-FDM-TS0027 - SET ANSI_NULLS ON STATEMENT MAY HAVE A DIFFERENT BEHAVIOR IN SNOWFLAKE **
--SET ANSI_NULLS ON
		-- DROP TABLE T_RTP_TABLE;
	END;
';