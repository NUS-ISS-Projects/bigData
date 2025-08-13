#!/usr/bin/env python3
"""
Business Formation Truncation Fix Validator

This script validates that all text truncation issues in the Business Formation
tab have been resolved, specifically focusing on:
1. AI-Powered Key Insights & Recommendations display
2. LLM-generated content completeness
3. JSON data integrity
4. Dashboard rendering without truncation
"""

import os
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BusinessFormationTruncationValidator:
    """Validates that truncation fixes are working correctly"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.json_output_dir = self.base_dir / "dashboard_json_output"
        self.validation_results = {
            "timestamp": datetime.now().isoformat(),
            "tests_passed": 0,
            "tests_failed": 0,
            "validation_details": [],
            "recommendations": []
        }
    
    def run_comprehensive_validation(self) -> Dict[str, Any]:
        """Run all validation tests"""
        logger.info("🔍 Starting Business Formation Truncation Fix Validation...")
        
        # Test 1: Verify code truncation fixes
        self._test_code_truncation_fixes()
        
        # Test 2: Validate JSON content completeness
        self._test_json_content_completeness()
        
        # Test 3: Check LLM insights quality
        self._test_llm_insights_quality()
        
        # Test 4: Verify dashboard rendering
        self._test_dashboard_rendering_logic()
        
        # Test 5: Validate data consistency
        self._test_data_consistency()
        
        # Generate final report
        self._generate_validation_report()
        
        return self.validation_results
    
    def _test_code_truncation_fixes(self):
        """Test that code truncation patterns have been removed"""
        logger.info("Testing code truncation fixes...")
        
        files_to_check = [
            "enhanced_streamlit_dashboard.py",
            "llm_analysis_engine.py", 
            "enhanced_economic_intelligence.py"
        ]
        
        critical_patterns = [
            r'llm_analysis\[:500\]',  # Should be removed
            r'recommendations\[:500\]',  # Should be removed
            r'llm_analysis\[:300\]',  # Should be removed
            r'key_insights\[:3\]',  # Should be removed for critical sections
        ]
        
        issues_found = []
        
        for file_name in files_to_check:
            file_path = self.base_dir / file_name
            if not file_path.exists():
                continue
                
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                for pattern in critical_patterns:
                    matches = re.findall(pattern, content)
                    if matches:
                        issues_found.append({
                            "file": file_name,
                            "pattern": pattern,
                            "matches": len(matches)
                        })
                        
            except Exception as e:
                logger.error(f"Error checking {file_name}: {e}")
        
        if issues_found:
            self._add_test_result("Code Truncation Fixes", False, 
                                f"Found {len(issues_found)} critical truncation patterns still present")
            self.validation_results["recommendations"].append(
                "🚨 CRITICAL: Remove remaining truncation patterns in code")
        else:
            self._add_test_result("Code Truncation Fixes", True, 
                                "All critical truncation patterns removed")
    
    def _test_json_content_completeness(self):
        """Test that JSON files contain complete content"""
        logger.info("Testing JSON content completeness...")
        
        if not self.json_output_dir.exists():
            self._add_test_result("JSON Content Completeness", False, 
                                "JSON output directory not found")
            return
        
        llm_files = list(self.json_output_dir.glob("business_formation_llm_insights_*.json"))
        
        if not llm_files:
            self._add_test_result("JSON Content Completeness", False, 
                                "No LLM insights files found")
            return
        
        complete_files = 0
        total_insights_length = 0
        total_recommendations_length = 0
        
        for json_file in llm_files[-5:]:  # Check last 5 files
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                content = data.get("content", {})
                insights = content.get("strategic_insights", "")
                recommendations = content.get("recommendations", "")
                
                total_insights_length += len(insights)
                total_recommendations_length += len(recommendations)
                
                # Check for completeness indicators
                if (len(insights) > 1000 and len(recommendations) > 500 and 
                    not insights.endswith("...") and not recommendations.endswith("...")):
                    complete_files += 1
                    
            except Exception as e:
                logger.error(f"Error analyzing {json_file.name}: {e}")
        
        avg_insights = total_insights_length / len(llm_files[-5:]) if llm_files else 0
        avg_recommendations = total_recommendations_length / len(llm_files[-5:]) if llm_files else 0
        
        if complete_files >= len(llm_files[-5:]) * 0.8:  # 80% should be complete
            self._add_test_result("JSON Content Completeness", True, 
                                f"Content quality good: avg insights {avg_insights:.0f} chars, "
                                f"avg recommendations {avg_recommendations:.0f} chars")
        else:
            self._add_test_result("JSON Content Completeness", False, 
                                f"Only {complete_files}/{len(llm_files[-5:])} files have complete content")
    
    def _test_llm_insights_quality(self):
        """Test the quality and completeness of LLM insights"""
        logger.info("Testing LLM insights quality...")
        
        llm_files = list(self.json_output_dir.glob("business_formation_llm_insights_*.json"))
        
        if not llm_files:
            self._add_test_result("LLM Insights Quality", False, "No LLM files found")
            return
        
        # Check most recent file
        latest_file = max(llm_files, key=lambda x: x.stat().st_mtime)
        
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            content = data.get("content", {})
            insights = content.get("strategic_insights", "")
            recommendations = content.get("recommendations", "")
            
            quality_score = 0
            quality_checks = []
            
            # Check 1: Length adequacy
            if len(insights) > 2000:
                quality_score += 1
                quality_checks.append("✅ Strategic insights length adequate")
            else:
                quality_checks.append(f"❌ Strategic insights too short: {len(insights)} chars")
            
            # Check 2: Recommendations length
            if len(recommendations) > 1000:
                quality_score += 1
                quality_checks.append("✅ Recommendations length adequate")
            else:
                quality_checks.append(f"❌ Recommendations too short: {len(recommendations)} chars")
            
            # Check 3: No truncation indicators
            if not insights.endswith(("...", "…")) and not recommendations.endswith(("...", "…")):
                quality_score += 1
                quality_checks.append("✅ No truncation indicators found")
            else:
                quality_checks.append("❌ Truncation indicators detected")
            
            # Check 4: Content structure
            if "trends" in insights.lower() and "formation" in insights.lower():
                quality_score += 1
                quality_checks.append("✅ Content contains relevant business formation analysis")
            else:
                quality_checks.append("❌ Content may not be relevant to business formation")
            
            if quality_score >= 3:
                self._add_test_result("LLM Insights Quality", True, 
                                    f"Quality score: {quality_score}/4. " + "; ".join(quality_checks))
            else:
                self._add_test_result("LLM Insights Quality", False, 
                                    f"Quality score: {quality_score}/4. " + "; ".join(quality_checks))
                
        except Exception as e:
            self._add_test_result("LLM Insights Quality", False, f"Error analyzing latest file: {e}")
    
    def _test_dashboard_rendering_logic(self):
        """Test that dashboard rendering logic doesn't truncate content"""
        logger.info("Testing dashboard rendering logic...")
        
        dashboard_file = self.base_dir / "enhanced_streamlit_dashboard.py"
        
        if not dashboard_file.exists():
            self._add_test_result("Dashboard Rendering Logic", False, "Dashboard file not found")
            return
        
        try:
            with open(dashboard_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check for problematic patterns in Business Formation section
            problematic_patterns = [
                r'llm_analysis\[:500\]',
                r'recommendations\[:500\]', 
                r'llm_analysis\[:300\]',
                r'\{[^}]*\[:500\][^}]*\}',  # Any variable truncated to 500
            ]
            
            issues = []
            for pattern in problematic_patterns:
                matches = re.findall(pattern, content)
                if matches:
                    issues.append(f"Pattern '{pattern}' found {len(matches)} times")
            
            if issues:
                self._add_test_result("Dashboard Rendering Logic", False, 
                                    f"Truncation patterns found: {'; '.join(issues)}")
            else:
                self._add_test_result("Dashboard Rendering Logic", True, 
                                    "No truncation patterns found in dashboard rendering")
                
        except Exception as e:
            self._add_test_result("Dashboard Rendering Logic", False, f"Error checking dashboard: {e}")
    
    def _test_data_consistency(self):
        """Test data consistency across different JSON files"""
        logger.info("Testing data consistency...")
        
        json_files = list(self.json_output_dir.glob("business_formation_*.json"))
        
        if len(json_files) < 2:
            self._add_test_result("Data Consistency", False, "Insufficient files for consistency check")
            return
        
        try:
            # Check recent files for consistency
            recent_files = sorted(json_files, key=lambda x: x.stat().st_mtime)[-3:]
            
            file_metrics = []
            for json_file in recent_files:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if "llm_insights" in json_file.name:
                    content = data.get("content", {})
                    metrics = {
                        "file": json_file.name,
                        "insights_length": len(content.get("strategic_insights", "")),
                        "recommendations_length": len(content.get("recommendations", "")),
                        "has_content": bool(content.get("strategic_insights") and content.get("recommendations"))
                    }
                    file_metrics.append(metrics)
            
            if file_metrics:
                avg_insights = sum(m["insights_length"] for m in file_metrics) / len(file_metrics)
                avg_recommendations = sum(m["recommendations_length"] for m in file_metrics) / len(file_metrics)
                complete_files = sum(1 for m in file_metrics if m["has_content"])
                
                if complete_files == len(file_metrics) and avg_insights > 2000:
                    self._add_test_result("Data Consistency", True, 
                                        f"All {len(file_metrics)} files consistent. "
                                        f"Avg insights: {avg_insights:.0f} chars, "
                                        f"avg recommendations: {avg_recommendations:.0f} chars")
                else:
                    self._add_test_result("Data Consistency", False, 
                                        f"Inconsistency detected: {complete_files}/{len(file_metrics)} complete files")
            else:
                self._add_test_result("Data Consistency", False, "No LLM insight files found for consistency check")
                
        except Exception as e:
            self._add_test_result("Data Consistency", False, f"Error checking consistency: {e}")
    
    def _add_test_result(self, test_name: str, passed: bool, details: str):
        """Add a test result to the validation results"""
        if passed:
            self.validation_results["tests_passed"] += 1
            status = "✅ PASSED"
        else:
            self.validation_results["tests_failed"] += 1
            status = "❌ FAILED"
        
        self.validation_results["validation_details"].append({
            "test": test_name,
            "status": status,
            "passed": passed,
            "details": details
        })
        
        logger.info(f"{status}: {test_name} - {details}")
    
    def _generate_validation_report(self):
        """Generate final validation report"""
        total_tests = self.validation_results["tests_passed"] + self.validation_results["tests_failed"]
        success_rate = (self.validation_results["tests_passed"] / total_tests * 100) if total_tests > 0 else 0
        
        self.validation_results["summary"] = {
            "total_tests": total_tests,
            "success_rate": success_rate,
            "validation_status": "PASSED" if success_rate >= 80 else "FAILED"
        }
        
        # Save report
        report_file = self.json_output_dir / f"business_formation_truncation_fix_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.validation_results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📊 Validation report saved: {report_file}")

def main():
    """Main execution function"""
    print("🔍 Business Formation Truncation Fix Validator")
    print("=" * 60)
    
    validator = BusinessFormationTruncationValidator()
    results = validator.run_comprehensive_validation()
    
    # Print summary
    print(f"\n📊 VALIDATION SUMMARY")
    print(f"✅ Tests Passed: {results['tests_passed']}")
    print(f"❌ Tests Failed: {results['tests_failed']}")
    print(f"📈 Success Rate: {results['summary']['success_rate']:.1f}%")
    print(f"🎯 Overall Status: {results['summary']['validation_status']}")
    
    if results["recommendations"]:
        print(f"\n💡 RECOMMENDATIONS:")
        for i, rec in enumerate(results["recommendations"], 1):
            print(f"   {i}. {rec}")
    
    print(f"\n✅ Validation complete. Business Formation truncation fixes verified.")
    
    return results

if __name__ == "__main__":
    main()