Feature: Load Transactional Links
# ==============================================================================
# CHANGE HISTORY
# ==============
#
# Date      Who Version Details
# --------  --- ------- --------------------------------------------------------
# 28.09.19  NS  1.0     First release.
#
# ==============================================================================

Transactional Links are also known as Non-Historised Links.

They are a special form of link - with additional payload and often carrying a weak hub (i.e. needing a column
as part of the primary key that could be a hub but is not, as the contents would be trivial or meaningless).
The payload never changes, i.e. is never updated. So it does not need a hashdiff and is insert only. The SQL
for insert does a left outer join (to stop double loads by mistake).

If we ever have a changing payload then the data would revert to being a satellite.

The tests below load a transactions table. Transactions are made against an account (a hub).
Regular rules for a link mean:
- primary key is the hub keys (account no) but we only have one hub here, we have to add transaction number
to the key as a local identifier. Note that transaction number on its own is enough to form the key, but
we follow the rules and concatenate account no and tx no
- no hashdiff is needed

# ------------------------------------------------------------------------------
# Test load empty transactional link.
# ------------------------------------------------------------------------------
  Scenario load empty transactional link
    Given an empty TLINK_TRANSACTION table
    And a populated STG_TRANSACTION table
      | CUSTOMER_ID | TRANSACTION_NUMBER | TRANSACTION_DATE | LOADDATE  | SOURCE | TYPE | AMOUNT   |
      | 1234       | 12345678           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2340.50  |
      | 1234       | 12345679           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 123.40   |
      | 1234       | 12345680           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2546.23  |
      | 1234       | 12345681           | 19-09-2019       | 21-09-2019 | SAP    | CR   | -123.40  |
      | 1235       | 12345682           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 37645.34 |
      | 1236       | 12345683           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 236.55   |
      | 1237       | 12345684           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 3567.34  |
    When I load the vault
    Then the TLINK_TRANSACTION_TABLE should be
      | TRANSACTION_PK          | CUSTOMER_PK  | TRANSACTION_NUMBER | TRANSACTION_DATE | LOADDATE  | SOURCE | TYPE | AMOUNT   | EFFECTIVE_FROM |
      | md5('1234\|\|12345678') | md5('1234') | 12345678           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2340.50  | 19-09-2019     |
      | md5('1234\|\|12345679') | md5('1234') | 12345679           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 123.40   | 19-09-2019     |
      | md5('1234\|\|12345680') | md5('1234') | 12345680           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2546.23  | 19-09-2019     |
      | md5('1234\|\|12345681') | md5('1234') | 12345681           | 19-09-2019       | 21-09-2019 | SAP    | CR   | -123.40  | 19-09-2019     |
      | md5('1235\|\|12345682') | md5('1235') | 12345682           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 37645.34 | 19-09-2019     |
      | md5('1236\|\|12345683') | md5('1236') | 12345683           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 236.55   | 19-09-2019     |
      | md5('1237\|\|12345684') | md5('1237') | 12345684           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 3567.34  | 19-09-2019     |


# ------------------------------------------------------------------------------
# Test load populated transactional link - one cycle.
# ------------------------------------------------------------------------------
  Scenario load populated transactional link
    Given a populated TLINK_TRANSACTION table
      | TRANSACTION_PK          | CUSTOMER_PK  | TRANSACTION_NUMBER | TRANSACTION_DATE | LOADDATE  | SOURCE | TYPE | AMOUNT   | EFFECTIVE_FROM |
      | md5('1234\|\|12345678') | md5('1234') | 12345678           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2340.50  | 19-09-2019     |
      | md5('1234\|\|12345679') | md5('1234') | 12345679           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 123.40   | 19-09-2019     |
      | md5('1234\|\|12345680') | md5('1234') | 12345680           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2546.23  | 19-09-2019     |
      | md5('1234\|\|12345681') | md5('1234') | 12345681           | 19-09-2019       | 21-09-2019 | SAP    | CR   | -123.40  | 19-09-2019     |
      | md5('1235\|\|12345682') | md5('1235') | 12345682           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 37645.34 | 19-09-2019     |
      | md5('1236\|\|12345683') | md5('1236') | 12345683           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 236.55   | 19-09-2019     |
      | md5('1237\|\|12345684') | md5('1237') | 12345684           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 3567.34  | 19-09-2019     |
    And a populated STG_TRANSACTION table
      | CUSTOMER_ID | TRANSACTION_NUMBER | TRANSACTION_DATE | LOADDATE  | SOURCE | TYPE | AMOUNT   |
      | 1234       | 12345685           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 3478.50  |
      | 1234       | 12345686           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10.00    |
      | 1235       | 12345687           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 1734.65  |
      | 1236       | 12345688           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 4832.56  |
      | 1237       | 12345689           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10000.00 |
      | 1238       | 12345690           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 6823.55  |
      | 1238       | 12345691           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 4578.34  |
    When I load the vault
    Then the TLINK_TRANSACTION_TABLE should be
      | TRANSACTION_PK          | CUSTOMER_PK  | TRANSACTION_NUMBER | TRANSACTION_DATE | LOADDATE  | SOURCE | TYPE | AMOUNT   | EFFECTIVE_FROM |
      | md5('1234\|\|12345678') | md5('1234') | 12345678           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2340.50  | 19-09-2019     |
      | md5('1234\|\|12345679') | md5('1234') | 12345679           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 123.40   | 19-09-2019     |
      | md5('1234\|\|12345680') | md5('1234') | 12345680           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2546.23  | 19-09-2019     |
      | md5('1234\|\|12345681') | md5('1234') | 12345681           | 19-09-2019       | 21-09-2019 | SAP    | CR   | -123.40  | 19-09-2019     |
      | md5('1235\|\|12345682') | md5('1235') | 12345682           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 37645.34 | 19-09-2019     |
      | md5('1236\|\|12345683') | md5('1236') | 12345683           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 236.55   | 19-09-2019     |
      | md5('1237\|\|12345684') | md5('1237') | 12345684           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 3567.34  | 19-09-2019     |
      | md5('1234\|\|12345685') | md5('1234') | 12345685           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 3478.50  | 20-09-2019     |
      | md5('1234\|\|12345686') | md5('1234') | 12345686           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10.00    | 20-09-2019     |
      | md5('1235\|\|12345687') | md5('1235') | 12345687           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 1734.65  | 20-09-2019     |
      | md5('1236\|\|12345688') | md5('1236') | 12345688           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 4832.56  | 20-09-2019     |
      | md5('1237\|\|12345689') | md5('1237') | 12345689           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10000.00 | 20-09-2019     |
      | md5('1238\|\|12345690') | md5('1238') | 12345690           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 6823.55  | 20-09-2019     |
      | md5('1238\|\|12345691') | md5('1238') | 12345691           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 4578.34  | 20-09-2019     |


# ------------------------------------------------------------------------------
# Test mistaken double load of same data set.
# ------------------------------------------------------------------------------
  Scenario erroneous duplicate load of transactional link
    Given a populated TLINK_TRANSACTION table
      | TRANSACTION_PK          | CUSTOMER_PK  | TRANSACTION_NUMBER | TRANSACTION_DATE | LOADDATE  | SOURCE | TYPE | AMOUNT   | EFFECTIVE_FROM |
      | md5('1234\|\|12345678') | md5('1234') | 12345678           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2340.50  | 19-09-2019     |
      | md5('1234\|\|12345679') | md5('1234') | 12345679           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 123.40   | 19-09-2019     |
      | md5('1234\|\|12345680') | md5('1234') | 12345680           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2546.23  | 19-09-2019     |
      | md5('1234\|\|12345681') | md5('1234') | 12345681           | 19-09-2019       | 21-09-2019 | SAP    | CR   | -123.40  | 19-09-2019     |
      | md5('1235\|\|12345682') | md5('1235') | 12345682           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 37645.34 | 19-09-2019     |
      | md5('1236\|\|12345683') | md5('1236') | 12345683           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 236.55   | 19-09-2019     |
      | md5('1237\|\|12345684') | md5('1237') | 12345684           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 3567.34  | 19-09-2019     |
      | md5('1234\|\|12345685') | md5('1234') | 12345685           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 3478.50  | 20-09-2019     |
      | md5('1234\|\|12345686') | md5('1234') | 12345686           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10.00    | 20-09-2019     |
      | md5('1235\|\|12345687') | md5('1235') | 12345687           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 1734.65  | 20-09-2019     |
      | md5('1236\|\|12345688') | md5('1236') | 12345688           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 4832.56  | 20-09-2019     |
      | md5('1237\|\|12345689') | md5('1237') | 12345689           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10000.00 | 20-09-2019     |
      | md5('1238\|\|12345690') | md5('1238') | 12345690           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 6823.55  | 20-09-2019     |
      | md5('1238\|\|12345691') | md5('1238') | 12345691           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 4578.34  | 20-09-2019     |
    And a populated STG_TRANSACTION table
      | CUSTOMER_ID | TRANSACTION_NUMBER | TRANSACTION_DATE | LOADDATE  | SOURCE | TYPE | AMOUNT   |
      | 1234       | 12345685           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 3478.50  |
      | 1234       | 12345686           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10.00    |
      | 1235       | 12345687           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 1734.65  |
      | 1236       | 12345688           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 4832.56  |
      | 1237       | 12345689           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10000.00 |
      | 1238       | 12345690           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 6823.55  |
      | 1238       | 12345691           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 4578.34  |
    When I load the vault
    Then the TLINK_TRANSACTION_TABLE should be
      | TRANSACTION_PK          | CUSTOMER_PK  | TRANSACTION_NUMBER | TRANSACTION_DATE | LOADDATE  | SOURCE | TYPE | AMOUNT   | EFFECTIVE_FROM |
      | md5('1234\|\|12345678') | md5('1234') | 12345678           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2340.50  | 19-09-2019     |
      | md5('1234\|\|12345679') | md5('1234') | 12345679           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 123.40   | 19-09-2019     |
      | md5('1234\|\|12345680') | md5('1234') | 12345680           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2546.23  | 19-09-2019     |
      | md5('1234\|\|12345681') | md5('1234') | 12345681           | 19-09-2019       | 21-09-2019 | SAP    | CR   | -123.40  | 19-09-2019     |
      | md5('1235\|\|12345682') | md5('1235') | 12345682           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 37645.34 | 19-09-2019     |
      | md5('1236\|\|12345683') | md5('1236') | 12345683           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 236.55   | 19-09-2019     |
      | md5('1237\|\|12345684') | md5('1237') | 12345684           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 3567.34  | 19-09-2019     |
      | md5('1234\|\|12345685') | md5('1234') | 12345685           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 3478.50  | 20-09-2019     |
      | md5('1234\|\|12345686') | md5('1234') | 12345686           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10.00    | 20-09-2019     |
      | md5('1235\|\|12345687') | md5('1235') | 12345687           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 1734.65  | 20-09-2019     |
      | md5('1236\|\|12345688') | md5('1236') | 12345688           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 4832.56  | 20-09-2019     |
      | md5('1237\|\|12345689') | md5('1237') | 12345689           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10000.00 | 20-09-2019     |
      | md5('1238\|\|12345690') | md5('1238') | 12345690           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 6823.55  | 20-09-2019     |
      | md5('1238\|\|12345691') | md5('1238') | 12345691           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 4578.34  | 20-09-2019     |

# ------------------------------------------------------------------------------
# Test load populated transactional link - two cycles.
# ------------------------------------------------------------------------------
  Scenario load populated transactional link
    Given a populated TLINK_TRANSACTION table
      | TRANSACTION_PK          | CUSTOMER_PK  | TRANSACTION_NUMBER | TRANSACTION_DATE | LOADDATE  | SOURCE | TYPE | AMOUNT   | EFFECTIVE_FROM |
      | md5('1234\|\|12345678') | md5('1234') | 12345678           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2340.50  | 19-09-2019     |
      | md5('1234\|\|12345679') | md5('1234') | 12345679           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 123.40   | 19-09-2019     |
      | md5('1234\|\|12345680') | md5('1234') | 12345680           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2546.23  | 19-09-2019     |
      | md5('1234\|\|12345681') | md5('1234') | 12345681           | 19-09-2019       | 21-09-2019 | SAP    | CR   | -123.40  | 19-09-2019     |
      | md5('1235\|\|12345682') | md5('1235') | 12345682           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 37645.34 | 19-09-2019     |
      | md5('1236\|\|12345683') | md5('1236') | 12345683           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 236.55   | 19-09-2019     |
      | md5('1237\|\|12345684') | md5('1237') | 12345684           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 3567.34  | 19-09-2019     |
    And a populated STG_TRANSACTION table
      | CUSTOMER_ID | TRANSACTION_NUMBER | TRANSACTION_DATE | LOADDATE  | SOURCE | TYPE | AMOUNT   |
      | 1234       | 12345685           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 3478.50  |
      | 1234       | 12345686           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10.00    |
      | 1235       | 12345687           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 1734.65  |
      | 1236       | 12345688           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 4832.56  |
      | 1237       | 12345689           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10000.00 |
      | 1238       | 12345690           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 6823.55  |
      | 1238       | 12345691           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 4578.34  |
    When I load the vault
    And the STG_TRANSACTION table is loaded
      | CUSTOMER_ID | TRANSACTION_NUMBER | TRANSACTION_DATE | LOADDATE  | SOURCE | TYPE | AMOUNT   |
      | 1234       | 12345692           | 21-09-2019       | 23-09-2019 | SAP    | CR   | 234.56   |
      | 1234       | 12345693           | 21-09-2019       | 23-09-2019 | SAP    | DR   | 30.00    |
      | 1236       | 12345694           | 21-09-2019       | 23-09-2019 | SAP    | CR   | 456.65   |
      | 1236       | 12345695           | 21-09-2019       | 23-09-2019 | SAP    | DR   | 453.98   |
      | 1237       | 12345696           | 21-09-2019       | 23-09-2019 | SAP    | CR   | 40000.00 |
      | 1239       | 12345697           | 21-09-2019       | 23-09-2019 | SAP    | DR   | 34.87    |
      | 1239       | 12345698           | 21-09-2019       | 23-09-2019 | SAP    | CR   | 4567.87  |
    And I load the vault
    Then the TLINK_TRANSACTION_TABLE should be
      | TRANSACTION_PK          | CUSTOMER_PK  | TRANSACTION_NUMBER | TRANSACTION_DATE | LOADDATE  | SOURCE | TYPE | AMOUNT   | EFFECTIVE_FROM |
      | md5('1234\|\|12345678') | md5('1234') | 12345678           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2340.50  | 19-09-2019     |
      | md5('1234\|\|12345679') | md5('1234') | 12345679           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 123.40   | 19-09-2019     |
      | md5('1234\|\|12345680') | md5('1234') | 12345680           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 2546.23  | 19-09-2019     |
      | md5('1234\|\|12345681') | md5('1234') | 12345681           | 19-09-2019       | 21-09-2019 | SAP    | CR   | -123.40  | 19-09-2019     |
      | md5('1235\|\|12345682') | md5('1235') | 12345682           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 37645.34 | 19-09-2019     |
      | md5('1236\|\|12345683') | md5('1236') | 12345683           | 19-09-2019       | 21-09-2019 | SAP    | CR   | 236.55   | 19-09-2019     |
      | md5('1237\|\|12345684') | md5('1237') | 12345684           | 19-09-2019       | 21-09-2019 | SAP    | DR   | 3567.34  | 19-09-2019     |
      | md5('1234\|\|12345685') | md5('1234') | 12345685           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 3478.50  | 20-09-2019     |
      | md5('1234\|\|12345686') | md5('1234') | 12345686           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10.00    | 20-09-2019     |
      | md5('1235\|\|12345687') | md5('1235') | 12345687           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 1734.65  | 20-09-2019     |
      | md5('1236\|\|12345688') | md5('1236') | 12345688           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 4832.56  | 20-09-2019     |
      | md5('1237\|\|12345689') | md5('1237') | 12345689           | 20-09-2019       | 22-09-2019 | SAP    | DR   | 10000.00 | 20-09-2019     |
      | md5('1238\|\|12345690') | md5('1238') | 12345690           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 6823.55  | 20-09-2019     |
      | md5('1238\|\|12345691') | md5('1238') | 12345691           | 20-09-2019       | 22-09-2019 | SAP    | CR   | 4578.34  | 20-09-2019     |
      | md5('1234\|\|12345692') | md5('1234') | 12345692           | 21-09-2019       | 23-09-2019 | SAP    | CR   | 234.56   | 21-09-2019     |
      | md5('1234\|\|12345693') | md5('1234') | 12345693           | 21-09-2019       | 23-09-2019 | SAP    | DR   | 30.00    | 21-09-2019     |
      | md5('1236\|\|12345694') | md5('1236') | 12345694           | 21-09-2019       | 23-09-2019 | SAP    | CR   | 456.65   | 21-09-2019     |
      | md5('1236\|\|12345695') | md5('1236') | 12345695           | 21-09-2019       | 23-09-2019 | SAP    | DR   | 453.98   | 21-09-2019     |
      | md5('1237\|\|12345696') | md5('1237') | 12345696           | 21-09-2019       | 23-09-2019 | SAP    | CR   | 40000.00 | 21-09-2019     |
      | md5('1239\|\|12345697') | md5('1239') | 12345697           | 21-09-2019       | 23-09-2019 | SAP    | DR   | 34.87    | 21-09-2019     |
      | md5('1239\|\|12345698') | md5('1239') | 12345698           | 21-09-2019       | 23-09-2019 | SAP    | CR   | 4567.87  | 21-09-2019     |
