/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import { ISchemaType } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import { flattenSchema } from 'components/AbstractWidget/SchemaEditor/schemaParser';
import If from 'components/If';
import { isComplexType } from 'components/AbstractWidget/SchemaEditor/SchemaHelpers';
import Paper from '@material-ui/core/Paper';
import TextBox from 'components/AbstractWidget/FormInputs/TextBox';
import Select from 'components/AbstractWidget/FormInputs/Select';
import Checkbox from '@material-ui/core/Checkbox';
import CheckBoxOutlineBlankIcon from '@material-ui/icons/CheckBoxOutlineBlank';
import CheckBox from '@material-ui/icons/CheckBox';
import Box from '@material-ui/core/Box';
import isObject from 'lodash/isObject';

const styles = (theme): StyleRules => {
  return {
    container: {
      height: 'auto',
    },
    schemaContainer: {
      width: '500px',
      margin: '20px',
      height: 'auto',
      '& >div': {
        width: '100%',
        height: 'auto',
        padding: '2px 10px',
        display: 'grid',
        minHeight: '36px',
        marginTop: '2px',
      },
    },
    menuItem: {
      minHeight: 'unset',
    },
    checkboxWrapper: {
      textAlign: 'center',
    },
  };
};

const CheckboxWrapper = withStyles(
  (): StyleRules => {
    return {
      root: {
        textAlign: 'center',
      },
    };
  }
)(Box);

const MapWrapper = withStyles(
  (): StyleRules => {
    return {
      root: {
        display: 'grid',
        gridTemplateColumns: '50px 100px',
        alignItems: 'center',
        gridGap: '10px',
      },
    };
  }
)(Box);

const SingleColumnWrapper = withStyles(() => {
  return {
    root: {
      width: '160px', // 50 + 100 + 10 from the mapwrapper
    },
  };
})(Box);

const Nullable = ({ field }) => {
  return (
    <CheckboxWrapper>
      <Checkbox
        checked={field.nullable}
        color="primary"
        checkedIcon={<CheckBox fontSize="small" />}
        icon={<CheckBoxOutlineBlankIcon fontSize="small" />}
      />
    </CheckboxWrapper>
  );
};

const schemaTypes = [
  'array',
  'boolean',
  'bytes',
  'date',
  'decimal',
  'double',
  'enum',
  'float',
  'int',
  'long',
  'map',
  'number',
  'record',
  'string',
  'time',
  'union',
];

interface ISchemaEditorProps extends WithStyles<typeof styles> {
  schema: ISchemaType;
}

const FieldWrapper = ({ field, children, style = {} }) => {
  const spacing = field.parent && Array.isArray(field.parent) ? field.parent.length * 10 : 0;
  let customStyles = {
    marginLeft: `${spacing}px`,
    gridTemplateColumns: `calc(100% - 100px - ${spacing + 10}px) calc(100px + ${(spacing + 10) *
      2}px)  `,
    width: `calc(100% - ${spacing - 10}px)`,
  };
  if (style && isObject(style)) {
    customStyles = {
      ...customStyles,
      ...style,
    };
  }
  return (
    <Paper elevation={2} key={field.name} style={customStyles}>
      {children}
    </Paper>
  );
};

const FieldInputWrapper = withStyles(() => {
  return {
    root: {
      display: 'grid',
      gridTemplateColumns: '75% 25%',
    },
  };
})(Box);

const FieldType = ({ field }) => {
  return (
    <FieldWrapper field={field}>
      <FieldInputWrapper>
        <TextBox onChange={() => {}} widgetProps={{ placeholder: 'name' }} value={field.name} />
        <Select
          value={field.type}
          onChange={() => {}}
          widgetProps={{ options: schemaTypes, dense: true }}
        />
      </FieldInputWrapper>
      <Nullable field={field} />
    </FieldWrapper>
  );
};

const RenderArraySubType = ({ field }) => {
  return (
    <FieldWrapper field={field}>
      <SingleColumnWrapper>
        <Select
          value={field.type}
          onChange={() => {}}
          widgetProps={{ options: schemaTypes, dense: true }}
        />
      </SingleColumnWrapper>
      <Nullable field={field} />
    </FieldWrapper>
  );
};

const RenderEnumSubType = ({ field }) => {
  return (
    <FieldWrapper field={field}>
      <TextBox value={field.symbol} onChange={() => {}} widgetProps={{ placeholder: 'symbol' }} />
    </FieldWrapper>
  );
};

const RenderMapSubType = ({ field }) => {
  let label = '';
  if (['map-keys-complex-type-root', 'map-keys-simple-type'].indexOf(field.displayType) !== -1) {
    label = 'Keys: ';
  }
  if (
    ['map-values-complex-type-root', 'map-values-simple-type'].indexOf(field.displayType) !== -1
  ) {
    label = 'Values: ';
  }
  return (
    <FieldWrapper field={field}>
      <MapWrapper>
        <span>{label}</span>
        <Select
          value={field.type}
          onChange={() => {}}
          widgetProps={{ options: schemaTypes, dense: true, inline: true }}
        />
      </MapWrapper>
      <Nullable field={field} />
    </FieldWrapper>
  );
};

const RenderUnionType = ({ field }) => {
  return (
    <FieldWrapper field={field}>
      <SingleColumnWrapper>
        <Select
          value={field.type}
          onChange={() => {}}
          widgetProps={{ options: schemaTypes, dense: true }}
        />
      </SingleColumnWrapper>
      <Nullable field={field} />
    </FieldWrapper>
  );
};

const RenderSubType = ({ field }) => {
  const spacing = field.parent && Array.isArray(field.parent) ? field.parent.length * 10 : 0;
  switch (field.displayType) {
    case 'array-simple-type':
    case 'array-complex-type':
    case 'array-complex-type-root':
      return <RenderArraySubType field={field} />;
    case 'enum-symbol':
      return <RenderEnumSubType field={field} />;
    case 'map-keys-complex-type-root':
    case 'map-keys-simple-type':
    case 'map-values-complex-type-root':
    case 'map-values-simple-type':
      return <RenderMapSubType field={field} />;
    case 'union-simple-type':
    case 'union-complex-type-root':
      return <RenderUnionType field={field} />;
    default:
      return (
        <div
          key={field.name}
          style={{
            marginLeft: `${spacing}px`,
            gridTemplateColumns: `calc(33% - ${spacing + 10}px) 34% 33%`,
          }}
        >
          <If condition={field.name}>
            <div>{field.name}</div>
          </If>
          <If condition={field.symbol}>{field.symbol}</If>
          <div>{field.type}</div>
          <div>{field.nullable ? 'nullable' : 'not-nullable'}</div>
        </div>
      );
  }
};

function SchemaEditor({ schema, classes }: ISchemaEditorProps) {
  if (!schema) {
    return null;
  }
  return (
    <div className={classes.container}>
      <h1>Schema Editor</h1>
      <h3> Schema Name: {schema[0].schema.name}</h3>
      <h3> Fields: </h3>
      <div className={classes.schemaContainer}>
        {flattenSchema(schema[0], ['etlSchemaBody']).map((field) => {
          if (
            ['record-field-simple-type', 'record-field-complex-type-root'].indexOf(
              field.displayType
            ) !== -1
          ) {
            return <FieldType field={field} />;
          }
          return <RenderSubType field={field} />;
        })}
      </div>
      <pre>{JSON.stringify(flattenSchema(schema[0]), null, 2)}</pre>
    </div>
  );
}

const StyledDemo = withStyles(styles)(SchemaEditor);
export default function SchemaEditorWrapper(props) {
  return (
    <ThemeWrapper>
      <StyledDemo {...props} />
    </ThemeWrapper>
  );
}
