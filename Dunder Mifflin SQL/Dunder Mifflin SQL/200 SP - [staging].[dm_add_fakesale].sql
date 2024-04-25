SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [staging].[dm_add_fakesale]
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	-----------------------------------
	-- TODOs
	-- Assign Affiliate
	-- Create Customer, Get ID
	-- Assign Employee Sale
	-- Assign Shipper
	-- Create Order, Get ID
	-- Create OrderDetails
	-- Update Inventory
	-- Mark Fake Sale Processed


	DECLARE @AffiliateID INT,
		@ShipperID INT,
		@EmployeeID INT,
		@CustomerID INT,
		@OrderID INT,
		@WhileID INT = 1,
		@WhileMAX INT,
		@RandWhole INT
	DECLARE @NewSales TABLE (
		[NewSaleID] INT,
		[FakeSaleID] INT,
		[Rand] FLOAT
		)
	DECLARE @Cust TABLE (
		[Name] VARCHAR(100),
		[Address] VARCHAR(200),
		[Company] VARCHAR(100),
		[Title] VARCHAR(100),
		[Phone] VARCHAR(50),
		[Addr] VARCHAR(100),
		[City] VARCHAR(100),
		[ST] VARCHAR(10),
		[PostalCode] VARCHAR(10)
		)
	DECLARE @Ord TABLE (
		[DeDuplID] INT IDENTITY(1, 1),
		[ProductID] INT,
		[Quantity] INT,
		[UnitPrice] [money],
		[Discount] [real]
		)

	INSERT INTO @NewSales (
		[NewSaleID],
		[FakeSaleID],
		[Rand]
		)
	SELECT [NewSaleID] = ROW_NUMBER() OVER (
			ORDER BY [FakeSaleID]
			),
		A.FakeSaleID,
		RAND(CHECKSUM(NEWID()))
	FROM staging.FakeSales A
	WHERE A.ProcessedDate IS NULL

	SELECT @WhileMAX = (
			SELECT COUNT(*)
			FROM @NewSales A
			)

    ----------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    ----------------------------------------------------------------------------

	WHILE @WhileID <= @WhileMAX
	BEGIN
		SELECT @RandWhole = CAST(A.[Rand] * 100 AS INT)
		FROM @NewSales A
		WHERE A.NewSaleID = @WhileID

		INSERT INTO @Cust (
			[Name],
			[Address],
			[Company],
			[Title],
			[Phone],
			[Addr],
			[City],
			[ST],
			[PostalCode]
			)
		SELECT [Name],
			[Address],
			[Company],
			[Title],
			[Phone] = CASE 
				WHEN B.[Phone] LIKE '%x%'
					THEN LEFT(B.[Phone], CHARINDEX('x', B.[Phone]) - 1)
				ELSE B.Phone
				END,
			[Addr] = LEFT(B.[Address], CHARINDEX(CHAR(10), B.[Address]) - 1),
			[City] = LEFT(RIGHT(B.[Address], LEN(B.[Address]) - CHARINDEX(CHAR(10), B.[Address])), CHARINDEX(',', RIGHT(B.[Address], LEN(B.[Address]) - CHARINDEX(CHAR(10), B.[Address]) - 1))),
			[ST] = SUBSTRING(B.[Address], CHARINDEX(',', B.[Address]) + 2, 2),
			[PostalCode] = RIGHT(B.[Address], 5)
		FROM @NewSales A
		JOIN staging.FakeSales F
			ON A.FakeSaleID = F.FakeSaleID
		OUTER APPLY OPENJSON(F.Customer, '$.Customer') WITH (
				[Name] VARCHAR(100),
				[Address] VARCHAR(200),
				[Company] VARCHAR(100),
				[Title] VARCHAR(100),
				[Phone] VARCHAR(50)
				) B
		WHERE A.NewSaleID = @WhileID

		INSERT INTO @Ord (
			[ProductID],
			[Quantity],
			[UnitPrice],
			[Discount]
			)
		SELECT B.[ProductID],
			B.[Quantity],
			I.UnitPrice,
			[Discount] = CASE 
				WHEN @RandWhole % 5 = 0
					THEN .05
				WHEN @RandWhole IN (1, 2, 3, 4)
					THEN .1
				WHEN @RandWhole = 77
					THEN .2
				ELSE 0
				END
		FROM @NewSales A
		JOIN staging.FakeSales F
			ON A.FakeSaleID = F.FakeSaleID
		OUTER APPLY OPENJSON(F.[Order]) WITH (
				[ProductID] INT,
				[Quantity] INT
				) B
		JOIN dbo.Inventory I
			ON B.ProductID = I.ProductID
				AND F.SaleDate BETWEEN I.StartDate AND COALESCE(I.EndDate, GETDATE())
		WHERE A.NewSaleID = @WhileID

		DELETE
		FROM O -- Python Faker duplicates ProductID and causes PK issues
		FROM @Ord O
		JOIN (
			SELECT O.ProductID,
				MAX(O.[DeDuplID]) AS [DeDuplID]
			FROM @Ord O
			GROUP BY O.ProductID
			HAVING COUNT(*) > 1
			) B
			ON O.ProductID = B.ProductID
				AND O.DeDuplID <> B.DeDuplID

		SELECT @AffiliateID = A.AffiliateID,
			@ShipperID = S.ShipperID,
			@EmployeeID = E.EmployeeID
		FROM @NewSales N
		JOIN dbo.Affiliates A
			ON A.AffiliateID = (CAST(N.[Rand] * 100 AS [int]) % 6) + 1 -- 0-5, so +1
		JOIN dbo.Shippers S
			ON S.ShipperID = (CAST((1 - N.[Rand]) * 100 AS [int]) % 3) + 1 -- 0-2, so +1, inverse so not always aligned
		JOIN staging.EmpSaleRate E
			ON N.[Rand] BETWEEN E.SaleRateMin AND E.SaleRateMax
		WHERE N.NewSaleID = @WhileID

		INSERT INTO dbo.Customers (
			AffiliateID,
			CompanyName,
			ContactName,
			ContactTitle,
			[Address],
			City,
			ST,
			PostalCode,
			Country,
			Phone,
			Fax
			)
		SELECT @AffiliateID,
			A.Company,
			A.[Name],
			A.Title,
			A.Addr,
			A.City,
			A.ST,
			A.PostalCode,
			'USA',
			A.Phone,
			A.Phone
		FROM @Cust A

		SET @CustomerID = SCOPE_IDENTITY()

		INSERT INTO dbo.Orders (
			CustomerID,
			EmployeeID,
			OrderDate,
			RequiredDate,
			ShippedDate,
			ShipperID,
			Freight,
			OrderTotal,
			DiscountTotal
			)
		SELECT @CustomerID,
			@EmployeeID,
			F.SaleDate,
			[RequiredDate] = DATEADD(DAY, (@RandWhole % 7) + 3, F.SaleDate),
			[ShippedDate] = DATEADD(DAY, (@RandWhole % 5), F.SaleDate),
			@ShipperID,
			[Freight] = CAST(N.[Rand] * 100 AS MONEY),
			[OrderTotal] = (
				SELECT SUM((CONVERT([decimal](10, 2), round([Quantity] * [UnitPrice] - ([Quantity] * [UnitPrice]) * [Discount], (2)))))
				FROM @Ord O
				),
			[DiscountTotal] = (
				SELECT SUM((CONVERT([decimal](10, 2), round(([Quantity] * [UnitPrice]) * [Discount], (2)))))
				FROM @Ord O
				)
		FROM @NewSales N
		JOIN staging.FakeSales F
			ON N.FakeSaleID = F.FakeSaleID
		WHERE N.NewSaleID = @WhileID

		SET @OrderID = SCOPE_IDENTITY()

		INSERT INTO dbo.OrderDetails (
			OrderID,
			ProductID,
			UnitPrice,
			Quantity,
			Discount
			)
		SELECT @OrderID,
			O.ProductID,
			O.UnitPrice,
			O.Quantity,
			O.Discount
		FROM @Ord O

		UPDATE F
		SET ProcessedDate = GETDATE()
		FROM staging.FakeSales F
		JOIN @NewSales N
			ON F.FakeSaleID = N.FakeSaleID
		WHERE N.NewSaleID = @WhileID

		DELETE
		FROM @Cust

		DELETE
		FROM @Ord

		SET @WhileID = @WhileID + 1
	END



    ----------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    ----------------------------------------------------------------------------
    ----------------------------------------------------------------------------

	IF @WhileMAX > 0 -- Don't run if none inserted
	BEGIN
		DECLARE @MaxSaleDateProc DATE = (
				SELECT MAX(F.SaleDate)
				FROM staging.FakeSales F
				)

		UPDATE I
		SET UnitsInStock = I.UnitsInStock - A.QuantitySold,
			EndDate = CASE 
				WHEN I.UnitsInStock - A.QuantitySold < 0
					THEN A.MAXOrderDate
				ELSE NULL
				END
		--SELECT I.*, A.QuantitySold
		FROM dbo.Inventory I
		JOIN (
			SELECT I.InventoryID,
				SUM(D.Quantity) AS QuantitySold,
				MAX(O.OrderDate) AS MAXOrderDate
			FROM dbo.Inventory I
			JOIN dbo.OrderDetails D
				ON I.ProductID = D.ProductID
			JOIN dbo.Orders O
				ON D.OrderID = O.OrderID
					AND O.OrderDate BETWEEN I.StartDate AND COALESCE(I.EndDate, GETDATE())
			WHERE O.OrderDate = @MaxSaleDateProc
			GROUP BY I.InventoryID
			) A
			ON A.InventoryID = I.InventoryID

		INSERT INTO dbo.Inventory (
			ProductID,
			StartDate,
			UnitPrice,
			UnitsInStock,
			UnitsOnOrder,
			ReorderLevel
			)
		SELECT I.ProductID,
			[StartDate] = DATEADD(DAY, 1, B.MaxEndDate),
			[UnitPrice] = CAST(I.UnitPrice + (
					I.UnitPrice * (RAND(CHECKSUM(NEWID())) * .1) * CASE 
						WHEN CAST(RAND(CHECKSUM(NEWID())) * 10 AS INT) % 2 = 0
							THEN - 1
						ELSE 1
						END
					) AS MONEY),
			[UnitsInStock] = ABS(I.UnitsInStock) + 400,
			I.UnitsOnOrder,
			I.ReorderLevel
		FROM dbo.Inventory I
		LEFT JOIN dbo.Inventory I2
			ON I.ProductID = I2.ProductID
				AND I2.IsActive = 1
		JOIN (
			SELECT I.ProductID,
				MAX(I.EndDate) AS MaxEndDate
			FROM dbo.Inventory I
			GROUP BY I.ProductID
			) B
			ON I.ProductID = B.ProductID
		WHERE 1 = 1
			AND I.IsActive = 0
			AND I2.InventoryID IS NULL -- Doesn't currently have an active
			AND I.EndDate = B.MaxEndDate
	END -- IF @WhileMAX > 0	
END;
GO
