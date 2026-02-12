# Fetch and Process Multiple Files Using ForEach in Azure Data Factory

This guide explains how to fetch and process multiple files dynamically
in **Azure Data Factory (ADF)** using a **ForEach** activity and a
**Copy Activity**.

------------------------------------------------------------------------

## Step 1: Add a ForEach Activity

-   Open your ADF pipeline.
-   Drag a **ForEach** activity onto the pipeline canvas.

------------------------------------------------------------------------

## Step 2: Create a Pipeline Parameter

-   Click **outside** the ForEach activity (pipeline background).

-   Go to the **Parameters** tab.

-   Create a new parameter with the following details:

    -   **Name:** `FileList`
    -   **Type:** Array
    -   **Value:**

``` json
[
  {
    "csv_relative_url": "folder1/file1.csv",
    "file_name": "file1.csv"
  },
  {
    "csv_relative_url": "folder2/file2.csv",
    "file_name": "file2.csv"
  }
]
```

### Parameter Explanation

-   `csv_relative_url` → Source file path
-   `file_name` → Target file name

------------------------------------------------------------------------

## Step 3: Configure the ForEach Activity

-   Click on the **ForEach** activity.
-   In the **Items** field, add the following dynamic content:

```{=html}
<!-- -->
```
    @pipeline().parameters.FileList

This enables looping through each object in the array.

------------------------------------------------------------------------

## Step 4: Add Copy Activity Inside ForEach

-   Open the **ForEach** activity.
-   Drag a **Copy Activity** inside it.

------------------------------------------------------------------------

## Step 5: Configure the Source

-   Open the **Source** tab in the Copy Activity.
-   In **Relative URL**, click **Add Dynamic Content**.
-   Use the following expression:

```{=html}
<!-- -->
```
    @item().csv_relative_url

This dynamically picks the source file path during each loop iteration.

------------------------------------------------------------------------

## Step 6: Configure the Sink

-   Open the **Sink** tab in the Copy Activity.
-   In **File Name**, click **Add Dynamic Content**.
-   Use the following expression:

```{=html}
<!-- -->
```
    @item().file_name

This dynamically assigns the target file name.

------------------------------------------------------------------------

## Step 7: How It Works

-   The **ForEach** activity loops through each object in the `FileList`
    array.
-   For every iteration:
    -   Source reads `csv_relative_url`
    -   Sink assigns `file_name`
-   The file is copied and stored in the configured **ADLS Gen2**
    location.

------------------------------------------------------------------------

## Step 8: Validate and Run

-   Click **Validate** to check for errors.
-   After successful validation:
    -   Click **Trigger → Run** to execute the pipeline.

------------------------------------------------------------------------

## Summary

This approach allows scalable, parameter-driven ingestion of multiple
files without hardcoding paths or filenames, making the pipeline
reusable and production-ready.
