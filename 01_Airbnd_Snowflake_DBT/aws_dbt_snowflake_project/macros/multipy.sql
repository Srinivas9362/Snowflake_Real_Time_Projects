{% macro multiply(x,y,z)%}
round({{x}} * {{y}},{{z}})
{% endmacro %}