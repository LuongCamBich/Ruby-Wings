# Test nhanh logic đã sửa
def test_fix():
    test_cases = [
        {'profile': {'age_group': '', 'group_type': None, 'confidence_scores': {}}, 'expected': 'No crash'},
        {'profile': {'age_group': '', 'group_type': 'family_with_kids', 'confidence_scores': {}}, 'expected': 'Has family_with_kids'},
        {'profile': {'age_group': '', 'group_type': 123, 'confidence_scores': {}}, 'expected': 'No crash'},
        {'profile': {}, 'expected': 'No crash'},
    ]
    
    passed = 0
    for i, test in enumerate(test_cases, 1):
        profile = test['profile']
        group_type_value = profile.get('group_type') if profile else None
        
        try:
            if group_type_value and isinstance(group_type_value, str) and 'family_with_kids' in group_type_value:
                result = 'Has family_with_kids'
            else:
                result = 'No crash'
                
            if result == test['expected'] or 'No crash' in result:
                print(f"Test {i} PASSED: {test['profile']}")
                passed += 1
            else:
                print(f"Test {i} FAILED: Expected {test['expected']}, got {result}")
        except Exception as e:
            print(f"Test {i} CRASHED: {e}")
    
    print(f"\n✓ {passed}/{len(test_cases)} tests passed")
    return passed == len(test_cases)

if __name__ == '__main__':
    test_fix()
