/**
 * Copyright © 2018 by Teradata.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

(function (window, document, angular, $) {

  if (typeof window === 'undefined' || !window) {
    throw new Error('DOM Window not present!')
  } else if (typeof document === 'undefined' || !document) {
    throw new Error('DOM Document not present!')
  } else if (typeof angular === 'undefined' || !angular) {
    throw new Error('Angular library not present!')
  } else if (typeof $ === 'undefined' || !$) {
    throw new Error('jQuery library not present!')
  }

  const app = angular.module('teradataScriptTO.module', []);

  app.controller('TeradataControllerScriptTO', function ($scope, $timeout) {

    /**
     * A wrapper function that delays execution so that
     * the Angular rendering cycle will have finished before
     * running the given function f.
     */
    const $delay = f => $timeout(f, 100);

    /**
     * Default separator for list-like objects.
     */
    const SEPARATOR = String.fromCharCode(0);

    /**
     * A private variable containing the function metadata.
     */
    let functionMetadata;

    /**
     * A private variable containing the function metadata.
     */
    let functionVersion = '';
    let functionType = '';

    /**
     * A private variable containing the function to run the given recipe.
     */
    let runFunction;

    /**
     * Dialog container.
     */
    const $dialog = $('#dialog');

    /**
     * Validation dialog container.
     */
    const $validationDialog = $('#dialog-validation');

    /**
     * Function metadata path - this path contains the JSON metadata of each function.
     */
    const FUNCTION_METADATA_PATH = '/plugins/AsterAnalytics/resource/data/';

    /**
     * Parameters to use for the dialog box.
     */
    const DIALOG_PARAMETERS = {
      modal: true,
      width: '33%',
      minHeight: 250,
      // close : function(){
      //     window.location.reload();
      // }
    }

    /**
     * Parameters to use for the dialog box CSS.
     */
    const DIALOG_CSS_PARAMETERS = {
      'max-height': '70vh',
      overflow: 'auto'
    }

    /**
     * Number of milliseconds per listen interval.
     */
    const LISTEN_INTERVAL = 50;

    let fileArraySize = 0;

    /**
     * Object keys that are repeatedly used in this script.
     */
    const KEYS = {
      LONG_DESCRIPTION: 'long_description',
      ALTERNATE_NAMES: 'alternateNames',
      TARGET_TABLE: 'targetTable',
      INPUT_TABLE: 'INPUTTABLE',
      INPUT_TABLE_ALTERNATIVE: 'INPUT_TABLE'
    }

    const ARGUMENT_TITLES = {
      // FILE_NAME: 'The name of the file to be installed',
      FILE_NAME: 'Specify the name of the input file to upload.',
      // FILE_ALIAS: 'The file alias to be used in the SQL statement',
      FILE_ALIAS: 'The Teradata Database needs an alias for the file, and a default alias is provided. You can change the default alias value, if desired, but ensure no two aliases are the same.',
      // FILE_LOCATION: 'The location of the file(s) to be installed, either the fully qualified path on the Teradata server, or a DSS Jupyter Notebook/Managed Folder',
      FILE_LOCATION: 'The location from where the file will be uploaded onto the Teradata Database.',
      // FILE_ADDRESS: 'The fully qualified file location on the Teradata Server',
      FILE_ADDRESS: 'The full path to the input file location on the Teradata Database server.',
      // FILE_FORMAT: 'Specifies whether a file is a TEXT or BINARY file',
      FILE_FORMAT: 'Specify whether the input is a TEXT or BINARY file.',
      // REPLACE_SCRIPT: 'Specifies whether a file will always be replaced',
      REPLACE_SCRIPT: 'Determines whether to replace an existing file on the Teradata Database or not. By default, if the box is checked and the file does not exist on the database, then the file will be installed.',
      SCRIPT_TYPE: 'The type of script to be used typically Python or R',
      // SCRIPT_TYPE: 'Specify the script language. The plugin currently supports scripts written in R or Python.',
      OTHER_COMMAND: 'Enter the Unix command to execute your script',
      // SCRIPT_ARGUMENTS: 'The arguments for the script, place one argument per box. Click on the (+) button to add more arguments',
      SCRIPT_ARGUMENTS: 'Specify any additional arguments, if needed by the script when executed on the command line. In case of more than one argument, click on the (+) button to specify each argument in a separate field box.',
      // ON: 'The ON Clause used as the input data for the script',
      ON: 'For the STO, the ON clause specifies the columns of a Teradata Database table that are streamed as input to your script. A default ON clause is provided, and it specifies using all columns of the recipe input dataset/table. To specify a different ON clause, click the "Customize the ON clause" button to edit the ON clause field.',
      // ON_CUSTOM: 'Determines whether the ON Clause should be modified',
      ON_CUSTOM: 'Enables customization of the default ON clause.',
      // HASH_BY: 'A HASH BY clause will cause the rows in the ON clause to be redistributed to AMPs based on the hash value of the column(s) specified',
      HASH_BY: 'The HASH BY clause groups table rows by the values of the specified column names, and distributes all rows of each group to a Teradata Database Access Module Processor (AMP; processing unit). Specify the column names you would like to hash their values by. If you specify a HASH BY clause, then you can also optionally order each group rows with the LOCAL ORDER BY clause.',
      // PARTITION_BY: 'A PARTITION BY clause will cause the STO to be executed against specific groups (partitions) based on the column(s) specified',
      PARTITION_BY: 'The PARTITION BY clause creates partitions of the input table data by the values of the specified column names. Specify the column names you would like to partition the table by. If you specify a PARTITION BY clause, then you can also optionally order the rows of each group with the ORDER BY clause.',
      // ORDER_BY: 'An ORDER BY clause specifies the order in which values in a group (partition) are sorted',
      ORDER_BY: 'The ORDER BY clause provides ordering of partitioned rows according to the input table columns that you specify in the clause. The ORDER BY clause can be only specified when you have specified a PARTITION BY clause, too.',
      // LOCAL_ORDER_BY: 'A LOCAL ORDER BY clause  orders the rows qualified on each AMP',
      LOCAL_ORDER_BY: 'The LOCAL ORDER BY clause provides ordering of the input table rows in the groups created with the HASH BY clause. The LOCAL ORDER BY clause can be only specified when you have specified a HASH BY clause, too.',
      // RETURNS_NAME: 'Specifies the name of the column(s) to be returned by the STO',
      RETURNS_NAME: 'Specify a name for each output argument returned by your script.',
      // RETURNS_TYPE: 'Specifies the data type of the column(s) to be returned by the STO',
      RETURNS_TYPE:'Specify the variable type of the corresponding output argument.',
      // SELECT_CUSTOM: 'Determines whether the SELECT (output) columns (data to be returned by the query) should be modified. Default is to SELECT all column(s) in the RETURNS clause',
      SELECT_CUSTOM: 'The default SELECT clause used by the plugin selects all output arguments of your script. Use this option to determine whether to modify the default SELECT behavior.',
      // SELECT_COLUMNS: 'Specifies the contents of a user customized SELECT statement (data to be returned by the query)',
      SELECT_COLUMNS: 'Specify explicitly the contents of the SELECT clause.',
      ADDITIONAL_CLAUSES: 'Specifies any additional clauses to the output such as a HAVING or QUALIFY clause'
      // ADDITIONAL_CLAUSES: 'Specifies the SQL code for any additional clause to be used when invoking the STO. Examples of such clauses include DELIMITER, QUOTECHAR, etc. For more details, see Chapter 27 of the Teradata Guide "SQL Functions, Operators, Expression, and Predicates."'

    }

    

    /** Regex for locating HTML entities. */
    const ENTITY_REGEX = /[\u00A0-\u9999<>\&]/gim

    /** Properly encodes an HTML entity. */
    function encodeRegex(i) {
      return '&#' + i.charCodeAt(0) + ';';
    }

    $.extend($scope, {

      /**
       * Shows a dialog.
       * 
       * This is for displaying Aster-side errors.
       */
      dialog: function (title, content, elem) {

        // [HACK] Sometimes, the title is "error" because of timing issues.
        // So we force it to succeed if the content is "Job succeeded".
        if (content === 'Job succeeded.') {
          title = 'Success'
        }

        $dialog.find('pre').text(content);
        $(elem).click(() => $('.ui-dialog-content').dialog('close'));
        $dialog.find('.other').empty().append(elem);

        $dialog
          .attr('title', title)
          .dialog(DIALOG_PARAMETERS);

        $dialog.css(DIALOG_CSS_PARAMETERS);

      },

      /**
       * Shows a dialog.
       * 
       * This is for displaying frontend validation errors.
       */
      validationDialog: function (html) {

        $validationDialog.find('div').html(html);
        $validationDialog.dialog(DIALOG_PARAMETERS);
        $validationDialog.css(DIALOG_CSS_PARAMETERS);

      },

      getParamType: function (v) {
          if (Array.isArray(v)) {
              return 'array';
          } else if (typeof v==='object' && v!==null && !(v instanceof Date)) {
              return 'dictionary';
          }
          return 'string'
      },

      hasNoPartitionBy(){
        if($scope.config.function.partitionby == null || $scope.config.function.partitionby == ''){
          console.log('No Part by')
          $scope.config.function.orderby = '';
          return true
        } else {
          console.log(' Part by')
          return false
        }
      },

      hasNoHashBy(){
        if($scope.config.function.hashby == null || $scope.config.function.hashby == ''){
          console.log('No hash by')
          $scope.config.function.localorderby = '';
          return true
        } else {
          console.log('Hash by')
          return false
        }
      },

      openTabs: function(evt, tabName) {
        // Declare all variables
        var i, tabcontent, tablinks;
        console.log(evt); 
        // Get all elements with class="tabcontent" and hide them
        tabcontent = document.getElementsByClassName("plugintabcontent");
        for (i = 0; i < tabcontent.length; i++) {
            tabcontent[i].style.display = "none";
        }
    
        // Get all elements with class="tablinks" and remove the class "active"
        tablinks = document.getElementsByClassName("tablinks");
        for (i = 0; i < tablinks.length; i++) {
            tablinks[i].className = tablinks[i].className.replace(" active", "");
        }
    
        // Show the current tab, and add an "active" class to the button that opened the tab
        document.getElementById(tabName).style.display = "block";
        evt.currentTarget.className += " active";
    },

      addMoreFilesClick: function () {
        $scope.config.function.files.push({});
        fileArraySize = $scope.config.function.files.length;
      },

      addReturnClause: function() {
          $scope.config.function.return_clause.push({});
      },

      removeReturnClause: function(index) {
          if (index > -1) {
              $scope.config.function.return_clause.splice(index, 1);
          }
      },

      addScriptArgument: function() {
          $scope.config.function.arguments.push({});
      },

      removeScriptArgument: function(index) {
          if (index > -1) {
              $scope.config.function.arguments.splice(index, 1);
          }
      },

      onFilenameChange() {
          let filename = $scope.config.function.script_filename || '';
          $scope.config.function.script_alias = (filename.substring(0,filename.lastIndexOf('.')) ||
              filename).replace(/("| |')/g, "");
      },

      onAdditionalFilenameChange: function(index) {
          if (!$scope || index < 0 ||
                  !$scope.config ||
                  !$scope.config.function ||
                  !$scope.config.function.files ||
                  index >= $scope.config.function.files.length) {
              return;
          }
          let filename = $scope.config.function.files[index].filename || '';
          $scope.config.function.files[index].file_alias = (filename
              .substring(0,filename.lastIndexOf('.')) || filename).replace(/("| |')/g, "");
      },

      onFileLocationChange() {
          $scope.config.function.script_filename = '';
          $scope.config.function.script_alias = '';
          $scope.config.function.command_type = 'other'
      },

      getOriginalON() {
         
        $scope.config.function.sql_on_clause = 'SELECT * FROM ' + $scope.config.function.input_table;

      },

      resetSelectToStar() {
        
       $scope.config.function.select_clause = '*';

     },

      onAdditionalFileLocationChange: function(index) {
          if (!$scope || index < 0 ||
                  !$scope.config ||
                  !$scope.config.function ||
                  !$scope.config.function.files ||
                  index >= $scope.config.function.files.length) {
              return;
          }
          $scope.config.function.files[index].filename = '';
          $scope.config.function.files[index].file_alias = '';
      },

      removeFile: function (index) {
        if (index > -1) {
          $scope.config.function.files.splice(index, 1);
        }
      },

      /**
       * Preprocesses the descriptions so that the special characters are correctly rendered.
       */
      preprocessDescriptions: function () {

        if (!functionMetadata || !functionMetadata.argument_clauses)
          return;

        functionMetadata.argument_clauses.forEach(
          x => {
            try {

              x.description = x.description
                ? x.description.replace(ENTITY_REGEX, encodeRegex)
                : ''

            } catch (e) { }
          }
        )

      },

      /**
       * Gets the function description from the static JSON metadata.
       */
      getFunctionDescription: function () {

        return (functionMetadata && KEYS.LONG_DESCRIPTION in functionMetadata) ?
          functionMetadata.long_description :
          '';

      },

      /**
       * Checks if there is a version mismatch in function_version
       */
      checkVersionMismatch: function () {


        // })
      },

      /**
       * Gets the description of the given argument from the static JSON metadata.
       */
      getArgumentDescription: function (argument) {
        return ARGUMENT_TITLES[argument]
      },

      getPermittedValues: function (i) {

        try {

          return (functionMetadata
            && functionMetadata.argument_clauses[i]
            && functionMetadata.argument_clauses[i].permittedValues)
            ? functionMetadata.argument_clauses[i].permittedValues
            : null;

        } catch (error) {

          return null;

        }


      },

      /**
       * Gets the schema of the unaliased inputs from the static JSON metadata.
       */
      getSchemaOfUnaliasedInputs: function (unaliasedInputsList) {

        if (!unaliasedInputsList
          || !unaliasedInputsList.values
          || !unaliasedInputsList.values.length)
          return [];

        const targetTableName = unaliasedInputsList.values[0];

        return $scope.inputschemas
          && targetTableName
          && targetTableName in $scope.inputschemas
          ? $scope.inputschemas[targetTableName]
          : []

      },

      findTableNameInArgumentsList: function (argumentsList, tableNameAlias) {
        //Get alternate names
        var tableNameAliases = [];
        tableNameAliases.push(tableNameAlias);
        functionMetadata.argument_clauses.map(argument => {
          if (argument.name.toUpperCase() === tableNameAlias) {
            if (KEYS.ALTERNATE_NAMES in argument) {
              argument.alternateNames.map(function (altname) { tableNameAliases.push(altname); })
            }
            console.log('tableNameAliases');
            console.log(tableNameAliases);
            // tableNameAliases.push(argument.name.toUpperCase());

          }
        })
        let potentialMatches = argumentsList
          .filter(arg => tableNameAliases.includes(arg.name.toUpperCase()));
        
        console.log('Find tablename');
        console.log(potentialMatches);
        console.log(argumentsList);
        if (potentialMatches.length) {
          console.log(potentialMatches);
          //console.log('We finally got here');
          return potentialMatches[0].value;
        }
        return ''

      },

      /**
       * Gets the function schema by joining and processing the metadata from the python backend 
       * and the static JSON file associated with the function.
       */
      getSchema: function (functionArgument, aliasedInputsList, unaliasedInputsList, argumentsList) {
        //console.log('Get Schema runs');
        aliasedInputsList = aliasedInputsList || []
        argumentsList = argumentsList || []
        console.log('getSchema');
        // console.log(functionMetadata)
        console.log(functionArgument);
        console.log(aliasedInputsList);
        console.log(unaliasedInputsList);
        console.log(argumentsList);
        const hasTargetTable = KEYS.TARGET_TABLE in functionArgument
        let targetTableName = ''
        var isAliasedInputsPopulated;
        var isInAliasedInputsList = false;
        var y = false;
        
        if (hasTargetTable) {
          
          const targetTableAlias = functionArgument.targetTable.toUpperCase();
          
          console.log('Table name');
          console.log(targetTableAlias);
          
          if (aliasedInputsList !== []) {
            isAliasedInputsPopulated = true;
            aliasedInputsList.map((input) => {
              if (input.name.toUpperCase() === targetTableAlias.toUpperCase()) {
                console.log('true');
                isInAliasedInputsList = true;
              }
            }
            )
          } else {
            isInAliasedInputsList = false;
          }
          
          const isAliased = isInAliasedInputsList;
          
          if (isAliased) {
            console.log('isAliased');
            let matchingInputs = aliasedInputsList.filter(input => targetTableAlias === input.name.toUpperCase());
            if (matchingInputs.length > 0) {
              
              targetTableName = matchingInputs[0].value;
            } else {
              
              targetTableName = $scope.findTableNameInArgumentsList(argumentsList, targetTableAlias);
            }


          } else {
            
            if (unaliasedInputsList.count && unaliasedInputsList.values && unaliasedInputsList.values.length) {
              console.log('Went to unaliased');

              targetTableName = unaliasedInputsList.values[0];
              
            }

            else {
              targetTableName = $scope.findTableNameInArgumentsList(argumentsList, targetTableAlias);
              console.log('Went to arguments');
              console.log(targetTableName);
            }


          }

        } else if (unaliasedInputsList.values && unaliasedInputsList.values.length > 0) {

          targetTableName = unaliasedInputsList.values[0]
          

        }

        if (!targetTableName || !$scope.inputschemas) {
          
          return [];
        }


        if (targetTableName && targetTableName in $scope.inputschemas) {
          
          return $scope.inputschemas[targetTableName];

        }

        return $scope.schemas;

      },

      /**
       * Checks whether or not we should show the partition order fields.
       * 
       * NOTE: Temporary code to not show partition 
       * and order by fields when there are no unaliased input dataset
       */
      shouldShowPartitionOrderFields: function (unaliasedInputsList) {
        return unaliasedInputsList && unaliasedInputsList.count > 0;
      },

      /**
       * Checks whether or not the given argument is an output table or not.
       */
      isArgumentOutputTable: function (functionArgument) {
        return functionArgument.isOutputTable
      },

      /**
       * Checks whether or not required arguments are present in the function metadata.
       */
      hasRequiredArguments: function () {

        if (!$scope.config.function.arguments || !$scope.config.function.arguments.length)
          return false

        return $scope.config.function.arguments.filter(x => x.isRequired).length > 0

      },

      /**
       * Checks whether or not optional arguments are present in the function metadata.
       */
      hasOptionalArguments: function () {

        const hasOptionalInputTable = $scope.config.function.required_input
          && $scope.config.function.required_input.length
          && ($scope.config.function.required_input.filter(x => !x.isRequired).length > 0);

        const hasOptionalArgument = $scope.config.function.arguments
          && $scope.config.function.arguments.length
          && ($scope.config.function.arguments.filter(x => !x.isRequired).length > 0);

        return hasOptionalInputTable || hasOptionalArgument;

      },

      /**
       * A function that listens for job results.
       * It accepts a callback function f that is executed after the results are received.
       * The presence or absence of the job results are listened to 
       * by using a simple setInterval (which checks for results every 50ms).
       * 
       * NOTE: This is a hack-ish implementation because we donot know how to listen to the job results properly.
       * If Dataiku publishes a proper DOM event then we should listen to that event instead.
       * But, currently, that event is not being published.
       */
      listenForResults: function (f) {
        const listener = setInterval(() => {
          const $jobResult = $('.recipe-editor-job-result')
          if ($jobResult.length && f()) {
            clearInterval(listener);
            $jobResult.remove();
          }
        }, LISTEN_INTERVAL)
      },

      /**
       * Function that validates all the input controls of the recipe.
       * 
       * Returns true if valid. If not, it shows an error dialog then returns false.
       */
      validate: function () {

        const invalids = []
        $('.ng-invalid:not(form,.ng-hide,div,#selectize-selectized,option)').each((i, x) => {
          // invalids.push($(x).parent().prev().text());
          invalids.push($(x)[0].id);
          console.log($(x)[0]);
        }
      )
        
        if (invalids.length) {
          $scope.validationDialog(`Please amend the following fields: <ul>${invalids.map(x => `<li>${x}</li>`).join('')}</ul>`)
          // $scope.validationDialog(`Please amend the fields marked in red`)
          return false
        }

        return true

      },

      /**
       * Function to execute the "proper" run workflow of an Aster recipe.
       * 
       * Firstly, the inputs are validated.
       * If the inputs are valid, it proceeds to the run phase 
       * which runs the original runFunction of the recipe (which we hijacked earlier).
       * It then listens for any results by using the listenForResults function.
       * Once the results are back, it displays it in a jQuery UI dialog box.
       */
      runThenListen: function () {

        if (!$scope.validate()) return;

        //console.log(functionVersion);
        //console.log($scope.config.function.function_version);
        // $scope.config.function.function_version = functionVersion;        
          
        console.log('Checking scope config');
        console.log($scope.config);
        runFunction();

        $scope.listenForResults(function () {
          //console.log('Got results');
          // //console.log(message);
          //console.log(functionVersion);
          //console.log($scope.config.function.function_version);
          const $results = $('.alert:not(.ng-hide):not(.messenger-message)')

          if ($results.length === 0) return false;

          try {

            const title = $results.get(0).classList[1].split('-')[1]

            if (!title || title === 'info')
              return false

            const result = $results.find('h4').text()

            const $jobLinkClone = $results.find('a').last().clone(true, true)

            $results.remove()
            $scope.dialog(title, result, $jobLinkClone)

            return true

          } catch (e) { }

          return false

        })

      },

      /**
       * Resolves a code-conflict issue between jQuery and Bootstrap.
       */
      initializeBootstrap: function () {
        if ($.fn.button && $.fn.button.noConflict)
          $.fn.bootstrapBtn = $.fn.button.noConflict()
      },

      /**
       * Communicates with Python backend 
       * and acquires necessary data to display in the recipe UI.
       */
      communicateWithBackend: function () {

        $scope.callPythonDo({}).then(
          data => $.extend($scope, data),
          () => {}
        ).then(
          () => { $scope.config.function.input_table = $scope.inputs.find(
                        (x => ('main' === x.role) && x.fullName) || {'fullName':''}).
                        fullName.split('.').pop();
                  if($scope.config.function.sql_on_clause == null || $scope.config.function.sql_on_clause == undefined){
                    $scope.getOriginalON()
                  }
                  if($scope.config.function.select_clause == null || $scope.config.function.select_clause == undefined){
                    $scope.resetSelectToStar()
                  }
                },
          () => {}
        );
      },

      /**
       * Activates the tabbing functionality of this recipe.
       */
      activateTabs: function () {
        try {
          $('#tabs').tabs('destroy')
        } catch (e) { }
        $('#tabs').tabs();
      },

      /**
       * Activates the informative tooltips of each input control.
       * Hijacks each tagsInput element so that it properly displays tooltips as well.
       */
      activateTooltips: function () {

        $('#main-container').tooltip();

        $delay(() => {

          $('.tagsinput').each((i, x) => {

            const original = $(x).prev().attr('data-original-title') || ''

            const title = original ?
              (original + '<br><br><b>(Press ENTER to add to list)</b>') :
              '<b>(Press ENTER to add to list)</b>'
            $(x).data({
              toggle: 'tooltip',
              container: 'body',
              placement: 'right',
              html: true,
              title: title,
              'original-title': title
            })
              .tooltip()

          });

        });

      },

      /**
       * Activates the multi-string input boxes.
       */
      activateMultiTagsInput: function () {
        try {

          $('input.teradata-tags').tagsInput({
            onChange: x => $(x).trigger('change'),
            defaultText: 'add param',
            delimiter: SEPARATOR
          });

        } catch (e) { }
      },

      /** 
       * Hijacks the current click handler of the recipe
       * and replaces it with the runThenListen function.
       * 
       * Stores the original recipe run function in the runFunction variable.
       */
      activateValidation: function () {

        const $runButton = $('.btn-run-main')
        runFunction = $._data($runButton.get(0), 'events').click[0].handler.bind($runButton.get(0));
        $runButton
          .off('click')
          .on('click', e => $scope.runThenListen());

      },

      /**
       * Hijacks the current UI and applies a few cosmetic improvements.
       */
      activateCosmeticImprovements: function () {

        const $a = $('.mainPane > div:first > div:first > div.recipe-settings-section2 > a');
        $a
          .text('Learn more about Teradata SCRIPT Table Operator')
          .css('color', 'orange')
          .attr('target', '_blank');
        $a.parent().css('text-align', 'center');
        // $('#main-container > div > div:nth-child(1) > div > select')[0].value = '';
        $('.dss-page,#main-container').css('display', 'block');
        // $('select:first, select:first > option').css('text-transform', 'capitalize');
        $('form').attr('novalidate', 'novalidate');

      },

      /**
       * Applies the custom changes to the default Dataiku UI needed for the Aster plugin to work.
       */
      activateUi: function () {

        $delay(() => {
          console.log('I can actually run');
          $scope.initializeBootstrap();
          console.log('Starting activation of cosmetic improvements')
          $scope.activateCosmeticImprovements();
          console.log('Starting activation of Tab activation')
          $scope.activateTabs();
          console.log('Starting activation of Multi tags input')
          $scope.activateMultiTagsInput();
          console.log('Starting activation validation')
          $scope.activateValidation();
          console.log('Initializing first tab')
          document.getElementById("defaultOpen").click();
          console.log('All is complete on activation')
          // Undefined
          // $scope.getOriginalON();
        });



      },

      cleanName: function (rawName) {
        return rawName.split('_').join(' ').toLowerCase()

      },

      formatConnectionSetting: function(key) {
          let formattedString = (key.match(/(.*[a-z])((?=[A-Z]).*)/) || []).slice(1)
            .join(" ") || key;
          return formattedString.charAt(0).toUpperCase() + formattedString.slice(1);
      },

      formatNameRuling: function(key) {
          let formattedString = key.match(/[A-Z]*[^A-Z]+/g).join(" ");
          return formattedString.charAt(0).toUpperCase() + formattedString.slice(1);
      },

      cleanKind: function (rawKind) {

        return rawKind ? `(${rawKind})` : ''

      },

      /**
       * Preprocess function metadata.
       */
      preprocessMetadata: function () {

        if (
          !functionMetadata
          || !$scope.config
          || !$scope.config.function
          || !$scope.config.function.arguments
          || !$scope.config.function.arguments.length) return;

        $delay(() => {

          // Re-arrange functions alphabetically.
          if ($scope.choices) {
            $scope.choices = $scope.choices.sort((a, b) => a.name.localeCompare(b.name))
          }

          // Properly bind default arguments.
          let i = 0;
          $scope.config.function.arguments.forEach(argument => {

            // Index each argument for easy access.
            argument.i = i;

            try {

              if (functionMetadata.argument_clauses[i]
                && typeof functionMetadata.argument_clauses[i].defaultValue != 'undefined') {
                argument.value = functionMetadata.argument_clauses[i].defaultValue;
              }

            } catch (e) {

            }

            ++i;

          });

          // Re-arrange argument order.
          $scope.config.function.arguments = [
            ...$scope.config.function.arguments.filter(x => x.datatype === 'TABLE_NAME'),
            ...$scope.config.function.arguments.filter(x => x.datatype !== 'TABLE_NAME'),
          ]

        });

      },

      /**
       * Initializes this plugin.
       */
      initialize: function () {
        console.log('Start initialize()')
        $scope.config.function = $scope.config.function || {};
        $scope.config.function.files = $scope.config.function.files || [];
        $scope.config.function.arguments = $scope.config.function.arguments || [{'value':''}];
        $scope.config.function.return_clause = $scope.config.function.return_clause || [{'name':'','type':''}];
        fileArraySize = $scope.config.function.files.length;

        $scope.communicateWithBackend();
        // if ($scope.config.function) {
        //   $scope.getFunctionMetadata($scope.config.function.name);
        // }

        // $scope.preprocessMetadata();
        $scope.activateUi();
        console.log('Initialize done')
      },

      /**
       * Initializes this plugin.
       */
      refresh: function (selectedFunction) {

        // $scope.getFunctionMetadata(selectedFunction);
        // $scope.preprocessMetadata();
        // //console.log($scope);
      }

    })

    $scope.initialize();


  });



})(window, document, angular, jQuery);