/***********************************
** Author: Aasrith Chenna
** Description: This File contains the create table functions for the datamodel and the sprocs required for the metadata driven 
* data pipeline orchestration architecture in the following blog 
* 
* https://www.linkedin.com/pulse/implementation-metadata-driven-orchestration-framework-aasrith-chenna/
************************************/

CREATE DATABASE MetadataOrchestration
USE MetadataOrchestration

CREATE TABLE Child_Pipeline_List (
	Child_Pipeline_Name nvarchar(50) primary key,
	Status nvarchar(50),
	Override_Flag int, 
);

CREATE TABLE Orchestration(
	Child_Pipeline_Name nvarchar(50) foreign key references Child_Pipeline_List(Child_Pipeline_Name),
	Parent_Pipeline_Name nvarchar(50),
	Logical_Prerequisite_Of_Parent nvarchar(50),
	Execution_Prerequisite_Of_Parent nvarchar(50)
);






SELECT * FROM Orchestration o 
SELECT * FROM Child_Pipeline_List cpl 

EXEC check_pipelines;
EXEC get_invalid_metadata_count 

-- RETURNS LIST OF PIPELINE NAMES WHERE EXECUTION PARENT AND LOGICAL PREREQS ARE MET 
ALTER PROCEDURE check_pipeline_condition
AS 
BEGIN 
	
	DECLARE @out TABLE (Child_Pipeline_Name nvarchar(50))
	
	INSERT INTO @out
		SELECT Child_Pipeline_Name  from Child_Pipeline_List cpl
		WHERE Child_Pipeline_Name IN 
		(
			SELECT Child_Pipeline_Name FROM Orchestration o 
			WHERE Parent_Pipeline_Name is NULL 
		)
		AND 
		Status is NULL 
	
	DECLARE @pipeline_logical_precedence TABLE (Child_Pipeline_Name nvarchar(50), Logical_Prerequisite_Of_Parent nvarchar(50))
	
	INSERT INTO 
	@pipeline_logical_precedence 
	SELECT DISTINCT(Child_Pipeline_Name), Logical_Prerequisite_Of_Parent
	FROM Orchestration o 
	WHERE 
	Logical_Prerequisite_Of_Parent IS NOT NULL
	
	SELECT Child_Pipeline_Name FROM @out 
	UNION 
	SELECT t.Child_Pipeline_Name FROM 
		(SELECT Orchestration.Child_Pipeline_Name , Orchestration.Parent_Pipeline_Name, 
			   Child_Pipeline_List.Status as Parent_Status
		FROM Orchestration 
		INNER JOIN Child_Pipeline_List 
		ON 
		Orchestration.Parent_Pipeline_Name = Child_Pipeline_List.Child_Pipeline_Name 
		WHERE 
		Orchestration.Child_Pipeline_Name 
		IN 
		(
			SELECT Child_Pipeline_Name 
			FROM Child_Pipeline_List 
			WHERE Status is NULL
		)
		AND 
		Child_Pipeline_List.Status = Execution_Prerequisite_Of_Parent ) as t
		WHERE 
		t.Child_Pipeline_Name IN (SELECT Child_Pipeline_Name 
									FROM @pipeline_logical_precedence 
									WHERE Logical_Prerequisite_Of_Parent = 'AND')
		GROUP BY t.Child_Pipeline_Name
		HAVING  COUNT(t.Child_Pipeline_Name) = (
			SELECT COUNT(o.Child_Pipeline_Name)
			FROM Orchestration o 
			GROUP BY o.Child_Pipeline_Name 
			HAVING o.Child_Pipeline_Name = t.Child_Pipeline_Name
		)
	UNION
	SELECT t.Child_Pipeline_Name FROM 
		(SELECT Orchestration.Child_Pipeline_Name , Orchestration.Parent_Pipeline_Name, 
			   Child_Pipeline_List.Status as Parent_Status
		FROM Orchestration 
		INNER JOIN Child_Pipeline_List 
		ON 
		Orchestration.Parent_Pipeline_Name = Child_Pipeline_List.Child_Pipeline_Name 
		WHERE 
		Orchestration.Child_Pipeline_Name 
		IN 
		(
			SELECT Child_Pipeline_Name 
			FROM Child_Pipeline_List 
			WHERE Status is NULL
		)
		AND 
		Child_Pipeline_List.Status = Execution_Prerequisite_Of_Parent) as t
		WHERE
		t.Child_Pipeline_Name IN (SELECT Child_Pipeline_Name 
									FROM @pipeline_logical_precedence 
									WHERE Logical_Prerequisite_Of_Parent = 'OR')
		GROUP BY t.Child_Pipeline_Name
		HAVING  COUNT(t.Child_Pipeline_Name) >= 1
											
			
END

		
	







/*
 * IF METADATA IS INVALID RETURN 1 
 * IF METADATA IS VALID BUT ALL PIPELINES HAVE NON NULL STATUS THEN ORCHESTRATION IS COMPLETE RETURN 2
 * IF NO PIPELINE WHICH HAS STATUS NULL HAS IT'S PRE REQUISITE CONDITIONS MET RETURN 3 
 * ELSE RETURN LIST OF CHILD PIPELINE NAMES TO RUN 
 */


ALTER PROCEDURE sp_check_pipelines 
AS 
  BEGIN 
      SET nocount ON 
	  SET ansi_warnings OFF 
      DECLARE @null_pipelines INT 
      DECLARE @invalid_pipeline_count INT 
      DECLARE @invalid_pipeline_logical_count INT 
      DECLARE @code INT 
		
      SET @invalid_pipeline_count = (SELECT COUNT(DISTINCT(o.Child_Pipeline_Name))
									FROM Orchestration o 
									INNER JOIN 
									Child_Pipeline_List cpl 
									ON 
									cpl.Child_Pipeline_Name = o.Parent_Pipeline_Name 
									WHERE o.Child_Pipeline_Name IN (
												SELECT Child_Pipeline_Name 
												FROM Child_Pipeline_List cpl2 
												WHERE cpl2.Status IS NOT NULL
									)
									AND cpl.Status  IN (NULL, 'Invoking', 'Running')) 
								     	
     SET @invalid_pipeline_logical_count = (
											SELECT COUNT(DISTINCT(Child_Pipeline_Name))
											FROM Orchestration o 
											GROUP BY 
											Child_Pipeline_Name 
											HAVING COUNT(DISTINCT(Logical_Prerequisite_Of_Parent))>1
										)
	 IF @invalid_pipeline_count IS NULL
	 	SET @invalid_pipeline_count = 0
	 IF @invalid_pipeline_logical_count IS NULL 
	 	SET @invalid_pipeline_logical_count = 0 
	 	
	 SET @invalid_pipeline_count = @invalid_pipeline_count + @invalid_pipeline_logical_count
      IF @invalid_pipeline_count > 0 
        BEGIN 
            SET @code = 1 

            SELECT @code 
            RETURN
        END 
      ELSE 
        BEGIN 
	        SET @null_pipelines = (
	        					   SELECT COUNT(Child_Pipeline_Name)
	        					   FROM Child_Pipeline_List cpl
	        					   WHERE Status is NULL
	        					   )
            IF @null_pipelines <= 0 
              BEGIN 
                  SET @code = 2 

                  SELECT @code 
                  RETURN
              END 
            ELSE 
              BEGIN 
                  DECLARE @out TABLE 
                    ( 
                       child_pipeline_name NVARCHAR(50) 
                    ) 

                  INSERT @out 
                  EXEC Check_pipeline_condition

                  IF (SELECT Count(*) 
                      FROM   @out) <= 0 
                    BEGIN 
                        SET @code = 3 

                        SELECT @code 
                        RETURN 
                    END 
                  ELSE 
                  	BEGIN  
	                  	UPDATE Child_Pipeline_List  
	                  	SET Status='Invoking' 
	                  	WHERE Child_Pipeline_Name IN (SELECT Child_Pipeline_Name  FROM @out)
	                  	
	                    SELECT child_pipeline_name 
	                    FROM   @out 
	                    RETURN
	                END
              END 
        END 
  END 

