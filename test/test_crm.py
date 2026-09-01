from crm.main import transform_row


# Test 1: Basic transformation
# This checks if the function correctly converts a row into a dictionary
def test_transform_row_basic():
    # This simulates one row from the database (tuple)
    row = ("2712345678", "John", "iPhone", "Joburg")

    # Call the function
    result = transform_row(row)

    # Check if each value is mapped correctly
    assert result["msisdn"] == "2712345678"
    assert result["name"] == "John"
    assert result["device"] == "iPhone"
    assert result["location"] == "Joburg"


# Test 2: Check if all required fields exist
# This makes sure the dictionary has the correct keys
def test_transform_row_contains_required_fields():
    row = ("2712345678", "John", "iPhone", "Joburg")

    result = transform_row(row)

    # Check if keys exist in the dictionary
    assert "msisdn" in result
    assert "name" in result
    assert "device" in result
    assert "location" in result


# Test 3: Handle missing values
# This checks if the function does not crash when values are missing
def test_transform_row_handles_missing_values():
    # Some values are None (missing)
    row = ("2712345678", None, "iPhone", None)

    result = transform_row(row)

    # The function should still return a dictionary without crashing
    assert result["msisdn"] == "2712345678"
    assert result["name"] is None
    assert result["device"] == "iPhone"
    assert result["location"] is None