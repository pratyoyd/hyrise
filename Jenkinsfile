import org.jenkinsci.plugins.pipeline.modeldefinition.Utils

full_ci = env.BRANCH_NAME == 'master' || pullRequest.labels.contains('FullCI')
// Due to their long runtime, we skip several tests in sanitizer builds.
tests_excluded_in_all_sanitizer_builds = 'SQLiteTestRunnerEncodings/*:TPCDSTableGeneratorTest.GenerateAndStoreRowCounts:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:*.TestTransactionConflicts'

// Dynamically loaded plugins currently violate the "one defintion rule" (ODR). We thus skip tests that load plugins.
// The issue is listed as #2632.
tests_excluded_in_addr_sanitizer_builds = 'MetaPluginsTest.*:PluginManagerTest.*'

try {
  node {
    stage ("Start") {
      // Check if the user who opened the PR is a known collaborator (i.e., has been added to a hyrise/hyrise team) or the Jenkins admin user
      def cause = currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause')[0]
      def jenkinsUserName = cause ? cause['userId'] : null

      if (jenkinsUserName != "admin" && env.BRANCH_NAME != "master") {
        try {
          withCredentials([usernamePassword(credentialsId: 'github', usernameVariable: 'GITHUB_USERNAME', passwordVariable: 'GITHUB_TOKEN')]) {
            env.PR_CREATED_BY = pullRequest.createdBy
            sh '''
              curl -s -I -H "Authorization: token ${GITHUB_TOKEN}" https://api.github.com/repos/hyrise/hyrise/collaborators/${PR_CREATED_BY} | head -n 1 | grep "204"
            '''
          }
        } catch (error) {
          stage ("User unknown.") {
            script {
              githubNotify context: 'CI Pipeline', status: 'FAILURE', description: 'User is not a collaborator.'
            }
          }
          throw error
        }
      }

      script {
        githubNotify context: 'CI Pipeline', status: 'PENDING'

        // Cancel previous builds
        if (env.BRANCH_NAME != 'master') {
          def jobname = env.JOB_NAME
          def buildnum = env.BUILD_NUMBER.toInteger()
          def job = Jenkins.instance.getItemByFullName(jobname)
          for (build in job.builds) {
            if (!build.isBuilding()) { continue; }
            if (buildnum == build.getNumber().toInteger()) { continue; }
            echo "Cancelling previous build " + build.getNumber().toString()
            build.doStop();
          }
        }
      }
    }
  }

  node('linux') {
    stage("Hostname") {
      // Print the hostname to let us know on which node the docker image was executed for reproducibility.
      sh "hostname"
    }

    // The empty '' results in using the default registry: https://index.docker.io/v1/
    docker.withRegistry('', 'docker') {
      def hyriseCI = docker.image('hyrise/hyrise-ci:23.10');
      hyriseCI.pull()

      // LSAN (executed as part of ASAN) requires elevated privileges. Therefore, we had to add --cap-add SYS_PTRACE.
      // Even if the CI run sometimes succeeds without SYS_PTRACE, you should not remove it until you know what you are doing.
      // See also: https://github.com/google/sanitizers/issues/764
      hyriseCI.inside("--cap-add SYS_PTRACE -u 0:0") {
        try {
          stage("Setup") {
            checkout scm

            // Check if ninja-build is installed. As make is sufficient to work with Hyrise, ninja-build is not
            // installed via install_dependencies.sh but is part of the hyrise-ci docker image.
            sh "ninja --version > /dev/null"

            // During CI runs, the user is different from the owner of the directories, which blocks the execution of git
            // commands since the fix of the git vulnerability CVE-2022-24765. git commands can then only be executed if
            // the corresponding directories are added as safe directories.
            sh '''
            git config --global --add safe.directory $WORKSPACE
            # Get the paths of the submodules; for each path, add it as a git safe.directory
            grep path .gitmodules | sed 's/.*=//' | xargs -n 1 -I '{}' git config --global --add safe.directory $WORKSPACE/'{}'
            '''

            sh "./install_dependencies.sh"

            cmake = 'cmake -DCI_BUILD=ON'

            // We don't use unity builds with clang-tidy (see below) and GCC 11 builds as it triggers a GoogleTest
            // issue (see https://github.com/google/googletest/issues/3552).
            unity = '-DCMAKE_UNITY_BUILD=ON'

            // To speed compiling up, we use ninja.
            ninja = '-GNinja'

            // With Hyrise, we aim to support the most recent compiler versions and do not invest a lot of work to
            // support older versions. We test LLVM 15 (oldest LLVM version shipped with Ubuntu 23.10 that works with
            // more recent libstdc++ versions) and GCC 11 (oldest version supported by Hyrise). We execute at least
            // debug runs for them. If you want to upgrade compiler versions, please update install_dependencies.sh,
            // DEPENDENCIES.md, and the documentation (README, Wiki).
            clang = '-DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++'
            clang15 = '-DCMAKE_C_COMPILER=clang-15 -DCMAKE_CXX_COMPILER=clang++-15'
            gcc = '-DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++'
            gcc11 = '-DCMAKE_C_COMPILER=gcc-11 -DCMAKE_CXX_COMPILER=g++-11'

            debug = '-DCMAKE_BUILD_TYPE=Debug'
            release = '-DCMAKE_BUILD_TYPE=Release'
            relwithdebinfo = '-DCMAKE_BUILD_TYPE=RelWithDebInfo'

            // jemalloc's autoconf operates outside of the build folder (#1413). If we start two cmake instances at the same time, we run into conflicts.
            // Thus, run this one (any one, really) first, so that the autoconf step can finish in peace.
            sh "mkdir clang-debug && cd clang-debug &&                                                   ${cmake} ${debug}          ${clang}  ${unity}  ${ninja} .. && ninja libjemalloc-build"

            // Configure the rest in parallel.
            // Note on the clang-debug-tidy stage: clang-tidy misses some flaws when running in a unity build. However, it runs very long and we agreed to life with that for now.
            // See: https://gitlab.kitware.com/cmake/cmake/-/issues/20058
            // TODO(Martin): update comment ... measure runtime
            sh "mkdir clang-debug-tidy && cd clang-debug-tidy &&                                         ${cmake} ${debug}          ${clang}             ${ninja} -DENABLE_CLANG_TIDY=ON .. &\
            mkdir clang-debug-unity-odr && cd clang-debug-unity-odr &&                                   ${cmake} ${debug}          ${clang}   ${unity}  ${ninja} -DCMAKE_UNITY_BUILD_BATCH_SIZE=0 .. &\
            mkdir clang-debug-disable-precompile-headers && cd clang-debug-disable-precompile-headers && ${cmake} ${debug}          ${clang}   ${unity}  ${ninja} -DCMAKE_DISABLE_PRECOMPILE_HEADERS=On .. &\
            mkdir clang-debug-addr-ub-leak-sanitizers && cd clang-debug-addr-ub-leak-sanitizers &&       ${cmake} ${debug}          ${clang}   ${unity}  ${ninja} -DENABLE_ADDR_UB_SANITIZATION=ON .. &\
            mkdir clang-release-addr-ub-leak-sanitizers && cd clang-release-addr-ub-leak-sanitizers &&   ${cmake} ${release}        ${clang}   ${unity}  ${ninja} -DENABLE_ADDR_UB_SANITIZATION=ON .. &\
            mkdir clang-relwithdebinfo-thread-sanitizer && cd clang-relwithdebinfo-thread-sanitizer &&   ${cmake} ${relwithdebinfo} ${clang}   ${unity}  ${ninja} -DENABLE_THREAD_SANITIZATION=ON .. &\
            mkdir clang-release && cd clang-release &&                                                   ${cmake} ${release}        ${clang}   ${unity}  ${ninja} .. &\
            mkdir gcc-debug && cd gcc-debug &&                                                           ${cmake} ${debug}          ${gcc}     ${unity}  ${ninja} .. &\
            mkdir gcc-release && cd gcc-release &&                                                       ${cmake} ${release}        ${gcc}               ${ninja} .. &\
            mkdir clang-15-debug && cd clang-15-debug &&                                                 ${cmake} ${debug}          ${clang15} ${unity}  ${ninja} .. &\
            mkdir gcc-11-debug && cd gcc-11-debug &&                                                     ${cmake} ${debug}          ${gcc11}             ${ninja} .. &\
            wait"
          }

          parallel clangDebug: {
            stage("clang-debug") {
              sh "cd clang-debug && ninja all -j \$(( \$(nproc) / 5))"
              sh "./clang-debug/hyriseTest clang-debug"
            }
          }, clang15Debug: {
            stage("clang-15-debug") {
              sh "cd clang-15-debug && ninja all -j \$(( \$(nproc) / 5))"
              sh "./clang-15-debug/hyriseTest clang-15-debug"
            }
          }, gccDebug: {
            stage("gcc-debug") {
              sh "cd gcc-debug && ninja all -j \$(( \$(nproc) / 5))"
              sh "cd gcc-debug && ./hyriseTest"
            }
          }, gcc11Debug: {
            stage("gcc-11-debug") {
               // We give more cores (ncores / 2.5) to GCC 11 as it is the only configuration that has issues with unity
               // builds (GoogleTest cannot be compiled). When switching to a more recent GCC version, this should be
               // evaluated again.
              sh "cd gcc-11-debug && ninja all -j \$(( \$(nproc) * 2 / 5))"
              sh "cd gcc-11-debug && ./hyriseTest"
            }
          }, lint: {
            stage("Linting") {
              sh "scripts/lint.sh"
            }
          }

          // We distribute the cores to processes in a way to even the running times. With an even distributions,
          // clang-tidy builds take up to 3h (galileo server). In addition to compile time, the distribution also
          // considers the long test runtimes of clangRelWithDebInfoThreadSanitizer (~50 minutes).
          parallel clangRelease: {
            stage("clang-release") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "cd clang-release && ninja all -j \$(( \$(nproc) / 10))"
                sh "./clang-release/hyriseTest clang-release"
                sh "./clang-release/hyriseSystemTest clang-release"
                sh "./scripts/test/hyriseConsole_test.py clang-release"
                sh "./scripts/test/hyriseServer_test.py clang-release"
                sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py clang-release"
                sh "./scripts/test/hyriseBenchmarkFileBased_test.py clang-release"
                sh "cd clang-release && ../scripts/test/hyriseBenchmarkTPCC_test.py ." // Own folder to isolate binary export tests
                sh "cd clang-release && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization

              } else {
                Utils.markStageSkippedForConditional("clangRelease")
              }
            }
          }, debugSystemTests: {
            stage("system-tests") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir clang-debug-system &&  ./clang-debug/hyriseSystemTest clang-debug-system"
                sh "mkdir gcc-debug-system &&  ./gcc-debug/hyriseSystemTest gcc-debug-system"
                sh "./scripts/test/hyriseConsole_test.py clang-debug"
                sh "./scripts/test/hyriseServer_test.py clang-debug"
                sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py clang-debug"
                sh "./scripts/test/hyriseBenchmarkFileBased_test.py clang-debug"
                sh "cd clang-debug && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization
                sh "cd clang-debug && ../scripts/test/hyriseBenchmarkJCCH_test.py ." // Own folder to isolate cached data
                sh "cd clang-debug && ../scripts/test/hyriseBenchmarkStarSchema_test.py ." // Own folder to isolate cached data
                sh "./scripts/test/hyriseConsole_test.py gcc-debug"
                sh "./scripts/test/hyriseServer_test.py gcc-debug"
                sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py gcc-debug"
                sh "./scripts/test/hyriseBenchmarkFileBased_test.py gcc-debug"
                sh "cd gcc-debug && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization
                sh "cd gcc-debug && ../scripts/test/hyriseBenchmarkJCCH_test.py ." // Own folder to isolate cached data
                sh "cd gcc-debug && ../scripts/test/hyriseBenchmarkStarSchema_test.py ." // Own folder to isolate cached data

              } else {
                Utils.markStageSkippedForConditional("debugSystemTests")
              }
            }
          }, clangDebugRunShuffled: {
            stage("clang-debug:test-shuffle") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir ./clang-debug/run-shuffled"
                sh "./clang-debug/hyriseTest clang-debug/run-shuffled --gtest_repeat=5 --gtest_shuffle"
                sh "./clang-debug/hyriseSystemTest clang-debug/run-shuffled --gtest_repeat=2 --gtest_shuffle"
              } else {
                Utils.markStageSkippedForConditional("clangDebugRunShuffled")
              }
            }
          }, clangDebugUnityODR: {
            stage("clang-debug-unity-odr") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                // Check if unity builds work even if everything is batched into a single compilation unit. This helps prevent ODR (one definition rule) issues.
                sh "cd clang-debug-unity-odr && ninja all -j \$(( \$(nproc) / 20))"
              } else {
                Utils.markStageSkippedForConditional("clangDebugUnityODR")
              }
            }
          }, clangDebugTidy: {
            stage("clang-debug:tidy") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                // We do not run tidy checks on the src/test folder, so there is no point in running the expensive clang-tidy for those files
                sh "cd clang-debug-tidy && ninja hyrise_impl hyriseBenchmarkFileBased hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkJoinOrder hyriseConsole hyriseServer hyriseMvccDeletePlugin hyriseUccDiscoveryPlugin -k 0 -j \$(( \$(nproc) / 5))"
              } else {
                Utils.markStageSkippedForConditional("clangDebugTidy")
              }
            }
          }, clangDebugDisablePrecompileHeaders: {
            stage("clang-debug:disable-precompile-headers") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                // Check if builds work even when precompile headers is turned off. Executing the binaries is unnecessary as the observed errors are missing includes.
                sh "cd clang-debug-disable-precompile-headers && ninja hyriseTest hyriseBenchmarkFileBased hyriseBenchmarkTPCH hyriseBenchmarkTPCDS hyriseBenchmarkJoinOrder hyriseConsole hyriseServer -k 0 -j \$(( \$(nproc) / 20))"
              } else {
                Utils.markStageSkippedForConditional("clangDebugDisablePrecompileHeaders")
              }
            }
          }, clangDebugAddrUBLeakSanitizers: {
            stage("clang-debug:addr-ub-sanitizers") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "cd clang-debug-addr-ub-leak-sanitizers && ninja hyriseTest hyriseSystemTest hyriseBenchmarkTPCH hyriseBenchmarkTPCC -j \$(( \$(nproc) / 20))"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:strict_init_order=1:detect_leaks=1,suppressions=resources/.asan-ignore.txt ./clang-debug-addr-ub-leak-sanitizers/hyriseTest --gtest_filter=-${tests_excluded_in_addr_sanitizer_builds} clang-debug-addr-ub-leak-sanitizers"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:strict_init_order=1:detect_leaks=1,suppressions=resources/.asan-ignore.txt ./clang-debug-addr-ub-leak-sanitizers/hyriseSystemTest --gtest_filter=-${tests_excluded_in_all_sanitizer_builds} clang-debug-addr-ub-leak-sanitizers"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:strict_init_order=1:detect_leaks=1,suppressions=resources/.asan-ignore.txt ./clang-debug-addr-ub-leak-sanitizers/hyriseBenchmarkTPCH -s .01 --verify -r 1"
              } else {
                Utils.markStageSkippedForConditional("clangDebugAddrUBLeakSanitizers")
              }
            }
          }, gccRelease: {
            if (env.BRANCH_NAME == 'master' || full_ci) {
              stage("gcc-release") {
                sh "cd gcc-release && ninja all -j \$(( \$(nproc) / 10))"
                sh "./gcc-release/hyriseTest gcc-release"
                sh "./gcc-release/hyriseSystemTest gcc-release"
                sh "./scripts/test/hyriseConsole_test.py gcc-release"
                sh "./scripts/test/hyriseServer_test.py gcc-release"
                sh "./scripts/test/hyriseBenchmarkJoinOrder_test.py gcc-release"
                sh "./scripts/test/hyriseBenchmarkFileBased_test.py gcc-release"
                sh "cd gcc-release && ../scripts/test/hyriseBenchmarkTPCC_test.py ." // Own folder to isolate binary export tests
                sh "cd gcc-release && ../scripts/test/hyriseBenchmarkTPCH_test.py ." // Own folder to isolate visualization
              }
            } else {
                Utils.markStageSkippedForConditional("gccRelease")
            }
          }, clangReleaseAddrUBLeakSanitizers: {
            stage("clang-release:addr-ub-sanitizers") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "cd clang-release-addr-ub-leak-sanitizers && ninja hyriseTest hyriseSystemTest hyriseBenchmarkTPCH hyriseBenchmarkTPCC -j \$(( \$(nproc) / 5))"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:use_odr_indicator=1:strict_init_order=1:detect_leaks=1,suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-leak-sanitizers/hyriseTest --gtest_filter=-${tests_excluded_in_addr_sanitizer_builds} clang-release-addr-ub-leak-sanitizers"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:use_odr_indicator=1:strict_init_order=1:detect_leaks=1,suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-leak-sanitizers/hyriseSystemTest --gtest_filter=-${tests_excluded_in_all_sanitizer_builds} clang-release-addr-ub-leak-sanitizers"
                sh "LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:use_odr_indicator=1:strict_init_order=1:detect_leaks=1,suppressions=resources/.asan-ignore.txt ./clang-release-addr-ub-leak-sanitizers/hyriseBenchmarkTPCH -s .01 --verify -r 100 --scheduler --clients 10 --cores \$(( \$(nproc) / 10))"
                sh "cd clang-release-addr-ub-leak-sanitizers && LSAN_OPTIONS=suppressions=resources/.lsan-ignore.txt ASAN_OPTIONS=strict_string_checks=1:detect_stack_use_after_return=1:check_initialization_order=1:use_odr_indicator=1:strict_init_order=1:detect_leaks=1,suppressions=resources/.asan-ignore.txt ../scripts/test/hyriseBenchmarkTPCC_test.py ." // Own folder to isolate binary export tests
              } else {
                Utils.markStageSkippedForConditional("clangReleaseAddrUBLeakSanitizers")
              }
            }
          }, clangRelWithDebInfoThreadSanitizer: {
            stage("clang-relwithdebinfo:thread-sanitizer") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "cd clang-relwithdebinfo-thread-sanitizer && ninja hyriseTest hyriseSystemTest hyriseBenchmarkTPCH -j \$(( \$(nproc) / 5))"
                sh "TSAN_OPTIONS=\"history_size=7 suppressions=resources/.tsan-ignore.txt\" ./clang-relwithdebinfo-thread-sanitizer/hyriseTest clang-relwithdebinfo-thread-sanitizer"
                sh "TSAN_OPTIONS=\"history_size=7 suppressions=resources/.tsan-ignore.txt\" ./clang-relwithdebinfo-thread-sanitizer/hyriseSystemTest --gtest_filter=-${tests_excluded_in_all_sanitizer_builds} clang-relwithdebinfo-thread-sanitizer"
                sh "TSAN_OPTIONS=\"history_size=7 suppressions=resources/.tsan-ignore.txt\" ./clang-relwithdebinfo-thread-sanitizer/hyriseBenchmarkTPCH -s .01 --verify -r 100 --scheduler --clients 10 --cores \$(( \$(nproc) / 10))"
              } else {
                Utils.markStageSkippedForConditional("clangRelWithDebInfoThreadSanitizer")
              }
            }
          }, clangDebugCoverage: {
            stage("clang-debug-coverage") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "./scripts/coverage.sh --generate_badge=true"
                sh "find coverage -type d -exec chmod +rx {} \\;"
                archive 'coverage_badge.svg'
                archive 'coverage_percent.txt'
                publishHTML (target: [
                  allowMissing: false,
                  alwaysLinkToLastBuild: false,
                  keepAll: true,
                  reportDir: 'coverage',
                  reportFiles: 'index.html',
                  reportName: "Llvm-cov_Report"
                ])
                script {
                  coverageChange = sh script: "./scripts/compare_coverage.sh", returnStdout: true
                  githubNotify context: 'Coverage', description: "$coverageChange", status: 'SUCCESS', targetUrl: "${env.BUILD_URL}/RCov_20Report/index.html"
                }
              } else {
                Utils.markStageSkippedForConditional("clangDebugCoverage")
              }
            }
          }

          parallel memcheckReleaseTest: {
            stage("memcheckReleaseTest") {
              // Runs after the other sanitizers as it depends on clang-release to be built.
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir ./clang-release-memcheck-test"
                // If this shows a leak, try --leak-check=full, which is slower but more precise
                sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --suppressions=resources/.valgrind-ignore.txt ./clang-release/hyriseTest clang-release-memcheck-test --gtest_filter=-NUMAMemoryResourceTest.BasicAllocate"
                sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --suppressions=resources/.valgrind-ignore.txt ./clang-release/hyriseBenchmarkTPCH -s .01 -r 1 --scheduler --cores 10"
                sh "valgrind --tool=memcheck --error-exitcode=1 --gen-suppressions=all --num-callers=25 --suppressions=resources/.valgrind-ignore.txt ./clang-release/hyriseBenchmarkTPCC -s 1 --scheduler --cores 10"
              } else {
                Utils.markStageSkippedForConditional("memcheckReleaseTest")
              }
            }
          }, tpchVerification: {
            stage("tpchVerification") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "./clang-release/hyriseBenchmarkTPCH --dont_cache_binary_tables -r 1 -s 1 --verify"
              } else {
                Utils.markStageSkippedForConditional("tpchVerification")
              }
            }
          }, tpchQueryPlans: {
            stage("tpchQueryPlans") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir -p query_plans/tpch; cd query_plans/tpch && ../../clang-release/hyriseBenchmarkTPCH --dont_cache_binary_tables -r 2 -s 10 --visualize && ../../scripts/plot_operator_breakdown.py ../../clang-release/"
                archiveArtifacts artifacts: 'query_plans/tpch/*.svg'
                archiveArtifacts artifacts: 'query_plans/tpch/operator_breakdown.pdf'
              } else {
                Utils.markStageSkippedForConditional("tpchQueryPlans")
              }
            }
          }, tpcdsQueryPlansAndVerification: {
            stage("tpcdsQueryPlansAndVerification") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir -p query_plans/tpcds; cd query_plans/tpcds && ln -s ../../resources; ../../clang-release/hyriseBenchmarkTPCDS --dont_cache_binary_tables -r 1 -s 1 --visualize --verify && ../../scripts/plot_operator_breakdown.py ../../clang-release/"
                archiveArtifacts artifacts: 'query_plans/tpcds/*.svg'
                archiveArtifacts artifacts: 'query_plans/tpcds/operator_breakdown.pdf'
              } else {
                Utils.markStageSkippedForConditional("tpcdsQueryPlansAndVerification")
              }
            }
          }, jobQueryPlans: {
            stage("jobQueryPlans") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                // In contrast to TPC-H and TPC-DS above, we execute the JoinOrderBenchmark from the project's root directoy because its setup script requires us to do so.
                sh "mkdir -p query_plans/job && ./clang-release/hyriseBenchmarkJoinOrder --dont_cache_binary_tables -r 1 --visualize && ./scripts/plot_operator_breakdown.py ./clang-release/ && mv operator_breakdown.pdf query_plans/job && mv *QP.svg query_plans/job"
                archiveArtifacts artifacts: 'query_plans/job/*.svg'
                archiveArtifacts artifacts: 'query_plans/job/operator_breakdown.pdf'
              } else {
                Utils.markStageSkippedForConditional("jobQueryPlans")
              }
            }
          }, ssbQueryPlans: {
            stage("ssbQueryPlans") {
              if (env.BRANCH_NAME == 'master' || full_ci) {
                sh "mkdir -p query_plans/ssb; cd query_plans/ssb && ../../clang-release/hyriseBenchmarkStarSchema --dont_cache_binary_tables -r 1 -s 1 --visualize && ../../scripts/plot_operator_breakdown.py ../../clang-release/"
                archiveArtifacts artifacts: 'query_plans/ssb/*.svg'
                archiveArtifacts artifacts: 'query_plans/ssb/operator_breakdown.pdf'
              } else {
                Utils.markStageSkippedForConditional("ssbQueryPlans")
              }
            }
          }

        } finally {
          sh "ls -A1 | xargs rm -rf"
          deleteDir()
        }
      }
    }
  }

  parallel clangDebugMacX64: {
    node('mac') {
      stage("clangDebugMacX64") {
        // We have experienced frequent network problems with this CI machine. So far, we have not found the cause.
        // Since we run this stage very late and it frequently fails due to network problems, we retry the stage three
        // times as (i) we can be rather sure that most problems with the current pull request have already been found
        // in earlier stages and fails in this stage are probably network issues, and (ii) we avoid re-runs of entire
        // Full CI runs that failed in the very last stage due to a single network issue.
        retry(3) {
          if (env.BRANCH_NAME == 'master' || full_ci) {
            try {
              checkout scm

              // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
              sh "git submodule update --init --recursive --jobs 4 --depth=1"

              // Build hyriseTest with macOS's default compiler (Apple clang) and run it.
              sh "mkdir clang-apple-debug && cd clang-apple-debug && /usr/local/bin/cmake ${debug} ${unity} .."
              sh "cd clang-apple-debug && ninja -j \$(sysctl -n hw.logicalcpu)"
              sh "./clang-apple-debug/hyriseTest"

              // Build Hyrise with a recent clang compiler version (as recommended for Hyrise on macOS) and run various tests.
              sh "mkdir clang-debug && cd clang-debug && /usr/local/bin/cmake ${debug} ${unity} -DCMAKE_C_COMPILER=/usr/local/opt/llvm@17/bin/clang -DCMAKE_CXX_COMPILER=/usr/local/opt/llvm@17/bin/clang++ .."
              sh "cd clang-debug && ninja -j \$(sysctl -n hw.logicalcpu)"
              sh "./clang-debug/hyriseTest"
              sh "./clang-debug/hyriseSystemTest --gtest_filter=\"-TPCCTest*:TPCDSTableGeneratorTest.*:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:*.CompareToSQLite/Line1*WithLZ4\""
              sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseConsole_test.py clang-debug"
              sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseServer_test.py clang-debug"
              sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseBenchmarkFileBased_test.py clang-debug"
            } finally {
              sh "ls -A1 | xargs rm -rf"
            }
          } else {
            Utils.markStageSkippedForConditional("clangDebugMacX64")
          }
        }
      }
    }
  }, clangReleaseMacArm: {
    // For this to work, we installed a native non-standard JDK (zulu) via brew. See #2339 for more details.
    node('mac-arm') {
      stage("clangReleaseMacArm") {
        if (env.BRANCH_NAME == 'master' || full_ci) {
          try {
            checkout scm

            // We do not use install_dependencies.sh here as there is no way to run OS X in a Docker container
            sh "git submodule update --init --recursive --jobs 4 --depth=1"

            // Build hyriseTest with macOS's default compiler (Apple clang) and run it.
            sh "mkdir clang-apple-release && cd clang-apple-release && cmake ${release} .."
            sh "cd clang-apple-release && ninja -j \$(sysctl -n hw.logicalcpu)"
            sh "./clang-apple-release/hyriseTest"

            // Build Hyrise with a recent clang compiler version (as recommended for Hyrise on macOS) and run various tests.
            // NOTE: These paths differ from x64 - brew on ARM uses /opt (https://docs.brew.sh/Installation)
            sh "mkdir clang-release && cd clang-release && cmake ${release} -DCMAKE_C_COMPILER=/opt/homebrew/opt/llvm@17/bin/clang -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm@17/bin/clang++ .."
            sh "cd clang-release && ninja -j \$(sysctl -n hw.logicalcpu)"

            // Check whether arm64 binaries are built to ensure that we are not accidentally running rosetta that
            // executes x86 binaries on arm.
            sh "file ./clang-release/hyriseTest | grep arm64"

            sh "./clang-release/hyriseTest"
            sh "./clang-release/hyriseSystemTest --gtest_filter=\"-TPCCTest*:TPCDSTableGeneratorTest.*:TPCHTableGeneratorTest.RowCountsMediumScaleFactor:*.CompareToSQLite/Line1*WithLZ4\""
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseConsole_test.py clang-release"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseServer_test.py clang-release"
            sh "PATH=/usr/local/bin/:$PATH ./scripts/test/hyriseBenchmarkFileBased_test.py clang-release"
          } finally {
            sh "ls -A1 | xargs rm -rf"
          }
        } else {
          Utils.markStageSkippedForConditional("clangReleaseMacArm")
        }
      }
    }
  }

  node {
    stage("Notify") {
      script {
        githubNotify context: 'CI Pipeline', status: 'SUCCESS'
        if (env.BRANCH_NAME == 'master' || full_ci) {
          githubNotify context: 'Full CI', status: 'SUCCESS'
        }
      }
    }
  }
} catch (error) {
  stage("Notify") {
    script {
      githubNotify context: 'CI Pipeline', status: 'FAILURE'
      if (env.BRANCH_NAME == 'master' || full_ci) {
        githubNotify context: 'Full CI', status: 'FAILURE'
      }
      if (env.BRANCH_NAME == 'master') {
        slackSend message: ":rotating_light: ALARM! Build on ${env.BRANCH_NAME} failed! - ${env.JOB_NAME} ${env.BUILD_NUMBER} (<${env.BUILD_URL}|Open>) :rotating_light:"
      }
    }
    throw error
  }
}
