class TilliT:

    _schedulerEndpoints = {
        "operations": "/operations",
        "routes": "/routes",
        "material_definitions": "/material-definitions",
        "materials_properties": "/material-properties",
        "segments": "/segments",
        "segment_equipments": "/segment-equipments",
        "segment_materials": "/segment-materials",
    }

    _scheduler_priority_map = {
        1: 'High',
        2: 'Medium',
        3: 'Low',
    }

    _schedulerStartDateEpoch = datetime(1970, 1, 1, 0, 0, 0, 0)

    def __init__(self, site: str, tenant: str, authBase64: str,  isStage: bool = False):
        self._site = site
        self._tenant = tenant
        self._baseURL = f"https://{tenant}.tillit{'-stage' if isStage else ''}.cloud/au/api"
        self._baseURLScheduler = f"{self._baseURL}/scheduler"
        self._baseURLSchedulerGraphQL = f"{self._baseURL}/scheduler/graphql"
        self._scheduler_data_id = 0
        self._scheduler_scenario_id = 0
        self._headers = {
            "Authorization": f"Basic {authBase64}",
            "Content-Type": "application/json"
        }

        self._set_data_template()

        if self._scheduler_data_id > 0 and self._scheduler_scenario_id > 0:
            self._baseURLScheduler = f"{self._baseURLScheduler}/{self._site}/{self._scheduler_data_id}"
        else:
            raise Exception("Failed to get Scheduler Data Template Id or Scenario Id")
            return

        print(f"""
        Site: {self.site}
        Tenant: {self.tenant}
        BaseURL: {self.baseURL}
        Scheduler BaseURL: {self._baseURLScheduler}
        Scheduler Scenario: {self._scheduler_scenario_id}
        """)
    
    def _set_data_template(self) -> bool:
        """
        Sets the scheduler data template ID and scenario ID for the current site.

        This method sends a GraphQL query to fetch the active scenario for the site specified
        by `self._site`. It extracts the `dataTemplate.id` and `scenario.id` from the response
        and assigns them to `self._scheduler_data_id` and `self._scheduler_scenario_id`.

        Returns:
            bool: True if both IDs are successfully retrieved and greater than zero, False otherwise.
        """
        payload = """query Scenarios { scenarios(where: { isLive: true, location: { code: "SiteCode" } }) { id dataTemplate { id }}}"""

        payload = payload.replace('SiteCode',self._site)
        data = self.fetch_scheduler_graphql({"query": payload})
        scenario = data["data"]["scenarios"][0]

        self._scheduler_data_id = int(scenario["dataTemplate"]["id"] if data else 0)
        self._scheduler_scenario_id = int(scenario["id"] if data else 0)

        return self._scheduler_data_id > 0 and self._scheduler_scenario_id >0

    def fetch_scheduler_graphql(self, payload) ->  dict:
        url = self._baseURLSchedulerGraphQL
        response = requests.post(url, headers=self._headers, json=payload)
        response.raise_for_status()
        return response.json()

    def fetch_scheduler(self, endpoint: str) ->  dict:
        """
        Sends a GET request to the scheduler API for the specified endpoint and returns the JSON response.

        This method constructs the full URL using the scheduler base URL and appends pagination parameters
        to retrieve a large dataset in one request. It raises an exception if the request fails.

        Args:
            endpoint (str): The relative endpoint path to query from the scheduler API.

        Returns:
            dict: The JSON response from the scheduler API.
        """
        
        url = f"{self._baseURLScheduler}{endpoint}"

        # avoid pagination
        url =f"{url}{'&' if ('?' in url) else '?'}page=0&size=100000&sort=id,asc"

        response = requests.get(url, headers=self._headers)
        response.raise_for_status()
        return response.json()

    def fetch_DO(self, endpoint: str) -> dict:
        """
        Sends a GET request to the DO (Data Operations) API for the specified endpoint and returns the JSON response.

        This method constructs the full URL using the DO base URL and appends pagination parameters
        to retrieve a large dataset in one request. It raises an exception if the request fails.

        Args:
            endpoint (str): The relative endpoint path to query from the DO API.

        Returns:
            dict: The JSON response from the DO API.
        """

        url = f"{self._baseURL}/{endpoint}"

        # avoid pagination
        url =f"{url}{'&' if ('?' in url) else '?'}page=0&size=100000&sort=id,asc"

        response = requests.get(url, headers=self._headers)
        response.raise_for_status()
        return response.json()

    def _scheduler_get_operations(self) -> pd.DataFrame:
        """
        Fetches the operations data from the scheduler endpoint and returns it as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing operations data.
        """
        return pd.DataFrame(self.fetch_scheduler(self._schedulerEndpoints["operations"]))

    def _scheduler_get_routes(self) -> pd.DataFrame:
        """
        Fetches the routing data from the scheduler endpoint and returns it as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing route definitions.
        """
        return pd.DataFrame(self.fetch_scheduler(self._schedulerEndpoints["routes"]))

    def _scheduler_get_equipment(self) -> pd.DataFrame:
        """
        Retrieves equipment metadata from scheduler.

        This method fetches equipment definitions associated with the current data template.
        The returned data includes equipment IDs, external identifiers, and descriptions.

        Returns:
        - pd.DataFrame: A DataFrame containing equipment details, sorted by external ID.
        Columns include:
            - id: Internal equipment identifier
            - externalId: Human-readable identifier used in scheduling
            - description: Textual description of the equipment

        """
        payload = """query equipments($where: FilterEquipmentInput!, $orderBy: [String]) {  equipments(where: $where, orderBy: $orderBy) {    id    externalId    description  }}"""

        payload = {
            "operationName": "equipments",
            "variables": {
                "where": {
                    "dataTemplate": {
                        "id": self._scheduler_data_id
                    }
                }
            },
            "query": payload}

        data = self.fetch_scheduler_graphql(payload)["data"]["equipments"]

        if data == None:
            return None
        data = pd.DataFrame(data)
        return data.sort_values(by="externalId").reset_index(drop=True)
    
    def _scheduler_get_materials(self, includeProperties:bool=False) -> pd.DataFrame:
        """
        Fetches material definitions from the scheduler endpoint and returns them as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing material definitions.
        """
        data = pd.DataFrame(self.fetch_scheduler(self._schedulerEndpoints["material_definitions"]))
        data["materialGroup"] = data["materialGroup"].apply(lambda x: extract(x, "externalId")).astype("string")
        data["externalId"] = data["externalId"].astype("string")
        data["description"] = data["description"].astype("string")

        selected_columns = ["externalId", "description", "materialGroup"]
        data = data[selected_columns]

        if not includeProperties:
            return data.fillna('')

        props = self._scheduler_get_materials_properties()

        merged_df = data \
            .merge(props, how="left", left_on="externalId", right_on="productCode") \
            .drop("productCode", axis=1)

        return merged_df.reset_index(drop=True).fillna('')
    
    def _scheduler_get_materials_properties(self) -> pd.DataFrame:
        """
        Fetches material definitions from the scheduler endpoint and returns them as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing material definitions.
        """
        data = pd.DataFrame(self.fetch_scheduler(self._schedulerEndpoints["materials_properties"]))
        data["productCode"] = data["materialDefinition"].apply(lambda x: extract(x, "externalId"))
        data["productCode"] = data["productCode"].astype("string")
        
        data = data.pivot(index='productCode', columns='externalId', values='value').reset_index()

        data = data.fillna('')

        return data.reset_index(drop=True)

    def _scheduler_get_segments(self) -> pd.DataFrame:
        """
        Fetches segment data from the scheduler endpoint and returns it as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing segment information.
        """
        return pd.DataFrame(self.fetch_scheduler(self._schedulerEndpoints["segments"]))

    def _scheduler_get_equipments(self) -> pd.DataFrame:
        """
        Fetches equipment data associated with segments from the scheduler endpoint and returns it as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing segment equipment data.
        """
        return pd.DataFrame(self.fetch_scheduler(self._schedulerEndpoints["segment_equipments"]))

    def _scheduler_get_segment_materials(self) -> pd.DataFrame:
        """
        Fetches material assignments for segments from the scheduler endpoint and returns them as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing segment-material relationships.
        """
        return pd.DataFrame(self.fetch_scheduler(self._schedulerEndpoints["segment_materials"]))

    def _scheduler_get_planned_order(self, excludeCompleted:bool = False, excludeItems:list[str] = []) -> pd.DataFrame:
        """
        Retrieves planned orders from scheduler.

        This method fetches all orders associated with the current scheduling scenario.
        It includes order metadata, status, order items, and order properties.

        Parameters:
        - excludeCompleted (bool): If True, filters out orders that are marked as completed.
        - excludeItems (list[str]): A list of order item IDs to exclude from the results.

        Returns:
        - pd.DataFrame: A DataFrame containing planned order details, including order items
        and their associated properties.

        Notes:
        - Use `orderItems` if you have more than one item allocated to each order.
        - This method does not apply filtering directly in the GraphQL query; filtering based on `excludeCompleted` and `excludeItems` is expected to be applied after fetching.
        """

        payload = """query orders($scenarioId: Int!, $ids: [Int]!) {  getOrdersForScenario(scenarioId: $scenarioId, ids: $ids) {    id    externalId    earliestStartDate    dueDate    priority    notes    status {      status      alias      code    }    orderItems {      id      invalid      invalidReason      allocated      quantity      quantityUnitOfMeasure      operationsDefinitionClass    }    orderProperties {      externalId      value    }  }}"""

        payload = {
            "operationName": "orders",
            "variables": {
                "scenarioId": self._scheduler_scenario_id,
                "ids": []
            },
            "query": payload
        }

        data = self.fetch_scheduler_graphql(payload)["data"]["getOrdersForScenario"]

        if data == None:
            return None
        data = pd.DataFrame(data)

        data['status'] = data['status'].apply(lambda x: extract(x, 'status'))
        data['orderItems'] = data['orderItems'].apply(lambda x: extract_fields(x, ["id", "quantity", "quantityUnitOfMeasure","operationsDefinitionClass"]))

        data['orderItemsId'] = data['orderItems'].apply(lambda x: x[0]['id'] if x else None).astype(np.int64)
        data['orderedQuantity'] = data['orderItems'].apply(lambda x: x[0]['quantity'] if x else None)
        data['orderUOM'] = data['orderItems'].apply(lambda x: x[0]['quantityUnitOfMeasure'] if x else None)
        data['ProductCode'] = data['orderItems'].apply(lambda x: x[0]['operationsDefinitionClass'].split(' - ')[0] if x else None)
        data['orderProperties'] = data['orderProperties'].apply(lambda x: extract_fields(x, ["externalId", "value"]) if pd.notna(x).any() else None)
        data['priority'] = data['priority'].apply(lambda x: self._scheduler_priority_map.get(x, ''))

        data['ProductCode']= data['ProductCode'].astype("string")
        data['notes']= data['notes'].astype("string")
        data['externalId']= data['externalId'].astype("string")

        selected_columns = ["id", "externalId", "earliestStartDate", "dueDate", "notes", "status", "orderItems", "orderProperties", "priority","orderItemsId", "orderedQuantity", "orderUOM", "ProductCode"]

        if excludeCompleted:
            data = data[~data["status"].isin(['COMPLETED', 'SUSPENDED', 'CANCELLED', 'READY', 'Complete'])]

        if excludeItems != []:
            data = data[~data["ProductCode"].isin(excludeItems)]

        return data[selected_columns].rename(columns={
            "externalId": "orderNumber"
        }).reset_index(drop=True)
    
    def _scheduler_get_scheduled_order(self) -> pd.DataFrame:
        """
        Retrieves scheduled order data from the scheduler,
        processes it, and returns a structured DataFrame with relevant scheduling details.

        Returns:
        - pd.DataFrame: A DataFrame containing scheduling information for each order item, including start/end times, scheduled quantity, duration, changeover time, and equipment used.
        """
        payload = """query getAllocations($scenarioId: Int!, $fromDate: String, $toDate: String) {  getAllocations(scenarioId: $scenarioId, fromDate: $fromDate, toDate: $toDate) {    version    allocations {      id      profileId      start      end      segmentId      orderItemId      quantity      duration      expectedDuration      durationLocked      assignments {        id        resourceId        resourceType        requirementId      }      allocatedPeriods {        start        end      }      changeover {        id        profileId        start        end        segmentId        orderItemId        quantity        duration        expectedDuration        durationLocked        linkedSegmentId        assignments {          id          resourceId          resourceType          requirementId        }        allocatedPeriods {          start          end        }      }    }  }}"""

        payload = {
            "operationName":"getAllocations",
            "variables":{
                    "scenarioId": self._scheduler_scenario_id,
                    "fromDate": None
                    ,"toDate": None
            },
            "query": payload
        }

        data = self.fetch_scheduler_graphql(payload)["data"]["getAllocations"]["allocations"]

        if data == None:
            return None
        
        data = pd.DataFrame(data)

        equipment = self._scheduler_get_equipment()
        
        data["StartDateTime"] = data["start"].apply(lambda x: self._schedulerStartDateEpoch + timedelta(seconds = int(x)/1000))
        data["EndDateTime"] = data["end"].apply(lambda x: self._schedulerStartDateEpoch + timedelta(seconds = int(x)/1000))

        data["Changeover_duration"] = data["changeover"].apply(lambda x: extract(x, "duration"))
        
        data["resourceIds"] = data["assignments"].apply(lambda x: [item["resourceId"] for item in x])
        data["Equipment"] = data["resourceIds"].apply(lambda x: equipment[equipment["id"].isin(x)]["externalId"].tolist())
        data["Equipment"] = data["Equipment"].apply(lambda x: ','.join(x) if x else None)
        data['orderItemId'] = data['orderItemId'].astype(np.int64)        

        selected_columns = ["orderItemId", "StartDateTime", "EndDateTime", "quantity", "duration", "expectedDuration", "durationLocked", "Changeover_duration", "Equipment"]

        return data[selected_columns].rename(columns={
                    "quantity":"ScheduledQuantity",
                }).reset_index(drop=True)

    def _do_get_completed_orders(self, orderNumbers: list[str]) -> list[str]:
        """
        Retrieves a list of completed order numbers from the external order instance API.

        Args:
            orderNumbers (list[str]): A list of order numbers to check for completion status.

        Returns:
            list[str]: A list of unique order numbers that have a status of 'COMPLETED'.
        """
        orders: List[str] = []

        batches = [orderNumbers[i:i + 80] for i in range(0, len(orderNumbers), 80)]
        for batch in batches:

            orderList = ",".join(batch)
            response = self.fetch_DO(endpoint=f"core/order-instances?status.equals=COMPLETED&orderNumber.in={orderList}")

            df = pd.DataFrame(response)

            if "orderNumber" in df.columns:
                orders += df["orderNumber"].unique().tolist()

        return list(set(orders))

#Exposed Functions
    
    def scheduler_get_bom_setup(self) -> pd.DataFrame:
        """
        Constructs a comprehensive Bill of Materials (BOM) setup by merging multiple scheduler datasets.

        This method performs the following steps:
        1. Fetches data from various scheduler endpoints: operations, routes, materials, segments, equipments, and segment-materials.
        2. Merges these datasets on relevant keys to create a unified DataFrame.
        3. Extracts and transforms nested fields using the `extract` function.
        4. Cleans and formats specific columns (e.g., replacing nulls, converting types).
        5. Selects and renames columns for clarity and usability.
        6. Sorts the final DataFrame by operation code, segment, route, and material code.

        Returns:
            pd.DataFrame: A cleaned and structured DataFrame representing the BOM setup, ready for analysis or export.
        """

        operations_df = self._scheduler_get_operations()
        routes_df = self._scheduler_get_routes()
        materials_df = self._scheduler_get_materials()
        segments_df = self._scheduler_get_segments()
        equipments_df = self._scheduler_get_equipments()
        segment_materials_df = self._scheduler_get_segment_materials()

        merged_df = operations_df \
        .merge(routes_df, how="left", left_on="operationCode", right_on="operationCode", suffixes=('', '_route')) \
        .merge(materials_df, how="left", left_on="operationCode", right_on="externalId", suffixes=('', '_material')) \
        .merge(segments_df, how="left", left_on=["operationCode","routeCode"], right_on=["operationCode","routeCode"], suffixes=('', '_segments')) \
        .merge(equipments_df, how="left", left_on=["operationCode","routeCode","segmentCode"], right_on=["operationCode","routeCode","segmentCode"], suffixes=('', '_equipment')) \
        .merge(segment_materials_df, how="left", left_on=["operationCode","routeCode","segmentCode"], right_on=["operationCode","routeCode","segmentCode"], suffixes=('', '_segmentMaterial')) 

        selected_columns = ["quantity", "operationCode", "externalId", "description", "materialGroup", "segmentCode", "route", "equipmentClassId"
                ,"equipmentClass","materialId","material","quantity_segmentMaterial"
                ,"quantityUnitOfMeasure","materialUse","fixedDuration", "rate","rateHour"
        ]

        merged_df['route'] = merged_df['route'].apply(lambda x: extract(x, 'routeCode'))
        merged_df['equipmentClassId'] = merged_df['equipmentClass'].apply(lambda x: extract(x, 'externalId'))
        merged_df['equipmentClass'] = merged_df['equipmentClass'].apply(lambda x: extract(x, 'description')).replace(["NaN", "null", "", np.nan, None], '')
        merged_df['materialId'] = merged_df['materialDefinition'].apply(lambda x: extract(x, 'externalId'))
        merged_df['material'] = merged_df['materialDefinition'].apply(lambda x: extract(x, 'description'))
        merged_df["fixedDuration"] = pd.to_numeric(merged_df["fixedDuration"].replace(["NaN", "null", "", np.nan], 0),errors='coerce')
        merged_df["rate"] = pd.to_numeric(merged_df["rate"].replace(["NaN", "null", "", np.nan], 0),errors='coerce')
        merged_df["rateHour"] = (merged_df["rate"] *60 *60)

        # before returning we should set the datatypes.
        merged_df = merged_df.astype({         
            "operationCode": "string",
            "externalId": "string",
            "description": "string",
            "materialGroup": "string",
            "segmentCode": "string",
            "route": "string",
            "quantity": "float",
            "quantity_segmentMaterial": "float",
            "quantityUnitOfMeasure": "string",
            "materialUse": "string",
            "equipmentClass": "string",
            "equipmentClassId": "string",
            "materialId": "string",
            "material": "string",
            "fixedDuration": "float",
            "rate": "float",
            "rateHour": "float"
        })
            
        merged_df = merged_df[selected_columns].rename(columns={
            "operationCode": "Operation Code",
            "externalId": "Operation External ID",
            "description": "Operation Description",
            "materialGroup": "Operation Material Group",
            "segmentCode": "Segment",
            "route": "Route",
            "quantity": "Quantity",
            "quantity_segmentMaterial": "Material Quantity",
            "quantityUnitOfMeasure": "Material Unit of Measure",
            "materialUse": "Material Use",
            "equipmentClass": "Equipment Class",
            "equipmentClassId": "Equipment Class ID",
            "materialId": "Material Code",
            "material": "Material Description",
            "fixedDuration": "Fixed Duration",
            "rate": "Rate",
            "rateHour": "Rate per Hour"
        }).sort_values(by=["Operation Code", "Segment", "Route", "Material Code"]).reset_index(drop=True)

        return merged_df

    def scheduler_get_materials(self, includeProperties=False) -> pd.DataFrame:
        """
        Retrieves a DataFrame containing materials.

        Parameters:
        - includeProperties (bool): If True, includes additional material properties (e.g. attributes) in the returned data.

        Returns:
        - pd.DataFrame: A DataFrame listing materials needed for scheduled production, optionally enriched with material properties.
        """
        return self._scheduler_get_materials(includeProperties)

    def scheduler_get_orders(self, excludeCompleted=True, excludeItems=[]) -> pd.DataFrame:

        """
        Constructs a view of production orders that are in the states:
        Planned, Scheduled, or Released.

        Parameters:
        - excludeCompleted (bool): If True, filters out orders that are marked as completed.
        - excludeItems (list): A list of item identifiers to exclude from the results.

        Returns:
        - pd.DataFrame: A merged DataFrame containing detailed information about planned and scheduled orders.
        """

        plannedOrders = self._scheduler_get_planned_order(excludeCompleted=excludeCompleted, excludeItems=excludeItems)
        scheduledOrder = self._scheduler_get_scheduled_order()

        merged_df = plannedOrders \
            .merge(scheduledOrder, how="inner", left_on="orderItemsId", right_on="orderItemId", suffixes=('', '_scheduled'))

        orderNumbers = merged_df["orderNumber"].unique().tolist() 
        doOrders = self._do_get_completed_orders(orderNumbers=orderNumbers)

        merged_df.loc[merged_df["orderNumber"].isin(doOrders), "status"] = "COMPLETED"

        merged_df = merged_df.drop(columns=['orderItemId','orderItemsId'],axis=1)
        merged_df.columns = [
            'Id', 'OrderNumber', 'EarliestStartDate', 'DueDate', 'Notes', 'Status', 'OrderItems', 'OrderProperties', 'Priority', 
            'OrderedQuantity', 'OrderUOM', 'ProductCode', 'StartDateTime', 'EndDateTime', 'ScheduledQuantity', 'Duration_Minutes', 
            'ExpectedDuration_Minutes', 'DurationLocked', 'ChangeoverDuration', 'Equipment']
        return merged_df

    @property
    def site(self) -> str:
        """Return the TilliT Site."""
        return self._site

    @property
    def tenant(self) -> str:
        """Return the TilliT Tenant."""
        return self._tenant
    
    @property
    def baseURL(self) -> str:
        """Return the TilliT DO base url."""
        return self._baseURL
    
    @property
    def baseURLScheduler(self) -> str:
        """Return the TilliT scheduler base url."""
        return self._baseURLScheduler
