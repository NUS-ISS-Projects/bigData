#!/usr/bin/env python3
"""
Business Formation AI Styling Test
Validates the AI-themed styling and functionality of the Business Formation tab
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BusinessFormationStylingTest:
    def __init__(self):
        self.json_output_dir = Path("dashboard_json_output")
        self.test_results = {
            "timestamp": datetime.now().isoformat(),
            "test_name": "Business Formation AI Styling Test",
            "tests_passed": 0,
            "tests_failed": 0,
            "test_details": []
        }
    
    def run_all_tests(self):
        """Run all styling and functionality tests"""
        logger.info("Starting Business Formation AI Styling Tests...")
        
        # Test 1: Check for recent LLM insights files
        self._test_llm_insights_generation()
        
        # Test 2: Validate LLM insights content quality
        self._test_llm_content_quality()
        
        # Test 3: Check JSON structure and completeness
        self._test_json_structure()
        
        # Test 4: Validate AI styling implementation
        self._test_ai_styling_implementation()
        
        # Test 5: Check data consistency
        self._test_data_consistency()
        
        # Generate final report
        self._generate_test_report()
    
    def _test_llm_insights_generation(self):
        """Test if LLM insights are being generated"""
        test_name = "LLM Insights Generation"
        logger.info(f"Running test: {test_name}")
        
        try:
            llm_files = list(self.json_output_dir.glob("business_formation_llm_insights_*.json"))
            
            if not llm_files:
                self._record_test_failure(test_name, "No LLM insights files found")
                return
            
            # Check for recent files (within last hour)
            recent_files = []
            current_time = datetime.now()
            
            for file_path in llm_files:
                file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                time_diff = (current_time - file_time).total_seconds()
                if time_diff < 3600:  # Within last hour
                    recent_files.append(file_path)
            
            if recent_files:
                self._record_test_success(test_name, f"Found {len(recent_files)} recent LLM insights files")
            else:
                self._record_test_failure(test_name, "No recent LLM insights files found")
                
        except Exception as e:
            self._record_test_failure(test_name, f"Error: {str(e)}")
    
    def _test_llm_content_quality(self):
        """Test the quality of LLM-generated content"""
        test_name = "LLM Content Quality"
        logger.info(f"Running test: {test_name}")
        
        try:
            llm_files = list(self.json_output_dir.glob("business_formation_llm_insights_*.json"))
            
            if not llm_files:
                self._record_test_failure(test_name, "No LLM insights files to test")
                return
            
            latest_file = max(llm_files, key=lambda x: x.stat().st_mtime)
            
            with open(latest_file, 'r') as f:
                data = json.load(f)
            
            content = data.get('content', {})
            insights = content.get('strategic_insights', '')
            recommendations = content.get('recommendations', '')
            
            quality_checks = {
                'insights_length': len(insights) > 500,
                'recommendations_length': len(recommendations) > 300,
                'contains_singapore': 'singapore' in insights.lower(),
                'contains_business': 'business' in insights.lower(),
                'actionable_recommendations': any(word in recommendations.lower() for word in ['should', 'recommend', 'implement']),
                'llm_model_specified': content.get('llm_model') == 'llama3.1:8b'
            }
            
            passed_checks = sum(quality_checks.values())
            total_checks = len(quality_checks)
            
            if passed_checks >= total_checks * 0.8:  # 80% pass rate
                self._record_test_success(test_name, f"Quality score: {passed_checks}/{total_checks}")
            else:
                self._record_test_failure(test_name, f"Low quality score: {passed_checks}/{total_checks}")
                
        except Exception as e:
            self._record_test_failure(test_name, f"Error: {str(e)}")
    
    def _test_json_structure(self):
        """Test JSON file structure and completeness"""
        test_name = "JSON Structure Validation"
        logger.info(f"Running test: {test_name}")
        
        try:
            bf_files = list(self.json_output_dir.glob("business_formation_*.json"))
            
            if not bf_files:
                self._record_test_failure(test_name, "No Business Formation JSON files found")
                return
            
            structure_issues = []
            
            for file_path in bf_files:
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    
                    # Check required fields
                    required_fields = ['page_name', 'timestamp', 'content']
                    missing_fields = [field for field in required_fields if field not in data]
                    
                    if missing_fields:
                        structure_issues.append(f"{file_path.name}: Missing fields {missing_fields}")
                    
                except json.JSONDecodeError:
                    structure_issues.append(f"{file_path.name}: Invalid JSON format")
                except Exception as e:
                    structure_issues.append(f"{file_path.name}: {str(e)}")
            
            if not structure_issues:
                self._record_test_success(test_name, f"All {len(bf_files)} files have valid structure")
            else:
                self._record_test_failure(test_name, f"Structure issues: {structure_issues}")
                
        except Exception as e:
            self._record_test_failure(test_name, f"Error: {str(e)}")
    
    def _test_ai_styling_implementation(self):
        """Test if AI styling is properly implemented"""
        test_name = "AI Styling Implementation"
        logger.info(f"Running test: {test_name}")
        
        try:
            dashboard_file = Path("enhanced_streamlit_dashboard.py")
            
            if not dashboard_file.exists():
                self._record_test_failure(test_name, "Dashboard file not found")
                return
            
            with open(dashboard_file, 'r') as f:
                content = f.read()
            
            styling_checks = {
                'ai_insight_card_class': '.ai-insight-card' in content,
                'ai_recommendation_card_class': '.ai-recommendation-card' in content,
                'gradient_background': 'linear-gradient(135deg, #1a1a2e' in content,
                'ai_content_class': '.ai-content' in content,
                'proper_z_index': 'z-index: 1' in content,
                'radial_gradient_effects': 'radial-gradient(circle' in content
            }
            
            passed_checks = sum(styling_checks.values())
            total_checks = len(styling_checks)
            
            if passed_checks == total_checks:
                self._record_test_success(test_name, "All AI styling elements implemented")
            else:
                failed_checks = [k for k, v in styling_checks.items() if not v]
                self._record_test_failure(test_name, f"Missing styling: {failed_checks}")
                
        except Exception as e:
            self._record_test_failure(test_name, f"Error: {str(e)}")
    
    def _test_data_consistency(self):
        """Test data consistency across files"""
        test_name = "Data Consistency"
        logger.info(f"Running test: {test_name}")
        
        try:
            bf_files = list(self.json_output_dir.glob("business_formation_*.json"))
            llm_files = [f for f in bf_files if 'llm_insights' in f.name]
            regular_files = [f for f in bf_files if 'llm_insights' not in f.name]
            
            consistency_issues = []
            
            # Check if we have both types of files
            if not llm_files:
                consistency_issues.append("No LLM insights files found")
            
            if not regular_files:
                consistency_issues.append("No regular Business Formation files found")
            
            # Check timestamp consistency (files should be generated around the same time)
            if llm_files and regular_files:
                latest_llm = max(llm_files, key=lambda x: x.stat().st_mtime)
                latest_regular = max(regular_files, key=lambda x: x.stat().st_mtime)
                
                time_diff = abs(latest_llm.stat().st_mtime - latest_regular.stat().st_mtime)
                
                if time_diff > 300:  # More than 5 minutes apart
                    consistency_issues.append(f"Large time gap between file types: {time_diff}s")
            
            if not consistency_issues:
                self._record_test_success(test_name, "Data consistency checks passed")
            else:
                self._record_test_failure(test_name, f"Issues: {consistency_issues}")
                
        except Exception as e:
            self._record_test_failure(test_name, f"Error: {str(e)}")
    
    def _record_test_success(self, test_name: str, details: str):
        """Record a successful test"""
        self.test_results["tests_passed"] += 1
        self.test_results["test_details"].append({
            "test_name": test_name,
            "status": "PASSED",
            "details": details
        })
        logger.info(f"✅ {test_name}: {details}")
    
    def _record_test_failure(self, test_name: str, details: str):
        """Record a failed test"""
        self.test_results["tests_failed"] += 1
        self.test_results["test_details"].append({
            "test_name": test_name,
            "status": "FAILED",
            "details": details
        })
        logger.error(f"❌ {test_name}: {details}")
    
    def _generate_test_report(self):
        """Generate and save the final test report"""
        total_tests = self.test_results["tests_passed"] + self.test_results["tests_failed"]
        success_rate = (self.test_results["tests_passed"] / total_tests * 100) if total_tests > 0 else 0
        
        self.test_results["summary"] = {
            "total_tests": total_tests,
            "success_rate": round(success_rate, 1),
            "overall_status": "PASSED" if success_rate >= 80 else "FAILED"
        }
        
        # Save report
        report_file = self.json_output_dir / f"business_formation_styling_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_file, 'w') as f:
            json.dump(self.test_results, f, indent=2)
        
        logger.info(f"Test report saved: {report_file}")
        
        # Print summary
        print("\n" + "="*70)
        print("🎨 BUSINESS FORMATION AI STYLING TEST SUMMARY")
        print("="*70)
        print(f"📊 Total Tests: {total_tests}")
        print(f"✅ Passed: {self.test_results['tests_passed']}")
        print(f"❌ Failed: {self.test_results['tests_failed']}")
        print(f"📈 Success Rate: {success_rate:.1f}%")
        print(f"🎯 Overall Status: {self.test_results['summary']['overall_status']}")
        print("\n📋 Test Details:")
        
        for test in self.test_results["test_details"]:
            status_icon = "✅" if test["status"] == "PASSED" else "❌"
            print(f"   {status_icon} {test['test_name']}: {test['details']}")
        
        print("="*70)

def main():
    """Main function to run the styling tests"""
    tester = BusinessFormationStylingTest()
    tester.run_all_tests()

if __name__ == "__main__":
    main()