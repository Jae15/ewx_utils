from ewx_utils.mawndb_classes.leaf_wetness import LeafWetness

def test_leafwt_hourly_leaf0_valid():
    leafwt_obj = LeafWetness(0.0, "HOURLY", "LEAF0")
    assert leafwt_obj.is_valid() == True

def test_leafwt_hourly_leaf0_valid():
    leafwt_obj = LeafWetness(0.75, "HOURLY", "LEAF0")
    assert leafwt_obj.is_valid() == True
    
def test_leafwt_hourly_leaf0_valid():
    leafwt_obj = LeafWetness(1.0, "HOURLY", "LEAF0")
    assert leafwt_obj.is_valid() == True
    
def test_leafwt_hourly_leaf0_invalid():
    leafwt_obj = LeafWetness(1.5, "HOURLY", "LEAF0")
    assert leafwt_obj.is_valid() == False
    
def test_leafwt_hourly_leaf1_valid():
    leafwt_obj = LeafWetness(0.9, "HOURLY", "LEAF1")
    assert leafwt_obj.is_valid() == True

def test_leafwt_hourly_leaf1_valid():
    leafwt_obj = LeafWetness(1.0, "HOURLY", "LEAF1")
    assert leafwt_obj.is_valid() == True

def test_leafwt_hourly_leaf1_invalid():
    leafwt_obj = LeafWetness(2.2, "HOURLY", "LEAF1")
    assert leafwt_obj.is_valid() == False

def test_leafwt_hourly_leaf0_wet():
    leafwt_obj = LeafWetness(0.5, "HOURLY", "LEAF0")
    assert leafwt_obj.is_wet() == True

def test_leafwt_hourly_leaf0_dry():
    leafwt_obj = LeafWetness(0.1, "HOURLY", "LEAF1")
    assert leafwt_obj.is_wet() == False
    
def test_leafwt_fivemin_leaf0_valid():
    leafwt_obj = LeafWetness(0.1, "FIVEMIN", "LEAF0")
    assert leafwt_obj.is_valid() == True

def test_leafwt_fivemin_leaf0_valid():
    leafwt_obj = LeafWetness(1.9, "FIVEMIN", "LEAF0")
    assert leafwt_obj.is_valid() == True
    
def test_leafwt_fivemin_leaf1_invalid():
    leafwt_obj = LeafWetness(-135, "FIVEMIN", "LEAF0")
    assert leafwt_obj.is_valid() == False
    
def test_leafwt_fivemin_leaf0_wet():
    leafwt_obj = LeafWetness(0.3, "FIVEMIN", "LEAF0")
    assert leafwt_obj.is_valid() == True

def test_leafwt_fivemin_leaf0_dry():
    leafwt_obj = LeafWetness(9999,"FIVEMIN", "LEAF1")
    assert leafwt_obj.is_wet() == False





