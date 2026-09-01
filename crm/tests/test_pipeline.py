import unittest
import datetime as dt

from pipeline import *


class TestPipeline(unittest.TestCase):

    def setUp(self):

        #fake extracted data from account table
        self.account_row = (
            1,"nhlaks","nhlaksdadawg@test.com",
            "27123456789",dt.datetime(1985,1,28)
        )

        #fake extracted data from address table
        self.address_row = (
            1, "Foresthill.dr", "JHB", "Carolina",
            "26", "RSA", dt.datetime(1985, 1, 28),
        )

        #fake extracted data from device table
        self.device_row = (
            0,1,"iphoneAir","cellphone","apple",
            dt.datetime(2020,9,5)
        ) 
#----------------Test edges cases for accounts----------------------------------------------------------------
    def test_transform_account_account_id(self):
        output = transform_account(self.account_row)
        self.assertEqual(output["account_id"], 1)
 
    def test_transform_account_owner_name(self):
        output = transform_account(self.account_row)
        self.assertEqual(output["owner_name"], "nhlaks")
 
    def test_transform_account_email(self):
        output = transform_account(self.account_row)
        self.assertEqual(output["email"], "nhlaksdadawg@test.com")
 
    def test_transform_account_phone_number(self):
        output = transform_account(self.account_row)
        self.assertEqual(output["phone_number"], "27123456789")
 
    def test_transform_account_modified_ts_is_string(self):
        output = transform_account(self.account_row)
        self.assertIsInstance(output["modified_ts"], str)
 
    def test_transform_account_source(self):
        output = transform_account(self.account_row)
        self.assertEqual(output["source"], "postgresql")
 
    def test_transform_account_has_all_keys(self):
        output = transform_account(self.account_row)
        expected_keys = {"account_id", "owner_name", "email", "phone_number", "modified_ts", "source"}
        self.assertEqual(set(output.keys()), expected_keys)

#----------------Test edges cases for address----------------------------------------------------------------
    def test_transform_address_account_id(self):
        output = transform_address(self.address_row)
        self.assertEqual(output["account_id"], 1)
 
    def test_transform_address_street_address(self):
        output = transform_address(self.address_row)
        self.assertEqual(output["street_address"], "Foresthill.dr")
 
    def test_transform_address_city(self):
        output = transform_address(self.address_row)
        self.assertEqual(output["city"], "JHB")
 
    def test_transform_address_state(self):
        output = transform_address(self.address_row)
        self.assertEqual(output["state"], "Carolina")
 
    def test_transform_address_postal_code(self):
        output = transform_address(self.address_row)
        self.assertEqual(output["postal_code"], "26")
 
    def test_transform_address_country(self):
        output = transform_address(self.address_row)
        self.assertEqual(output["country"], "RSA")
 
    def test_transform_address_modified_ts_is_string(self):
        output = transform_address(self.address_row)
        self.assertIsInstance(output["modified_ts"], str)
 
    def test_transform_address_source(self):
        output = transform_address(self.address_row)
        self.assertEqual(output["source"], "postgresql")
 
    def test_transform_address_has_all_keys(self):
        output = transform_address(self.address_row)
        expected_keys = {
            "account_id", "street_address", "city", "state",
            "postal_code", "country", "modified_ts", "source",
        }
        self.assertEqual(set(output.keys()), expected_keys)

#----------------Test edges cases for devices----------------------------------------------------------------
    def test_transform_device_device_id(self):
        output = transform_device(self.device_row)
        self.assertEqual(output["device_id"], 0)
 
    def test_transform_device_account_id(self):
        output = transform_device(self.device_row)
        self.assertEqual(output["account_id"], 1)
 
    def test_transform_device_device_name(self):
        output = transform_device(self.device_row)
        self.assertEqual(output["device_name"], "iphoneAir")
 
    def test_transform_device_device_type(self):
        output = transform_device(self.device_row)
        self.assertEqual(output["device_type"], "cellphone")
 
    def test_transform_device_device_os(self):
        output = transform_device(self.device_row)
        self.assertEqual(output["device_os"], "apple")
 
    def test_transform_device_modified_ts_is_string(self):
        output = transform_device(self.device_row)
        self.assertIsInstance(output["modified_ts"], str)
 
    def test_transform_device_source(self):
        output = transform_device(self.device_row)
        self.assertEqual(output["source"], "postgresql")
 
    def test_transform_device_has_all_keys(self):
        output = transform_device(self.device_row)
        expected_keys = {
            "device_id", "account_id", "device_name",
            "device_type", "device_os", "modified_ts", "source",
        }
        self.assertEqual(set(output.keys()), expected_keys)
    


if __name__ == "__main__":
    unittest.main()