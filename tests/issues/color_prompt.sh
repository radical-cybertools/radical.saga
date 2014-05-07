 
function color_my_prompt() {

    local __git_branch_color="\[\033[31m\]"
    local __git_branch='`git branch 2> /dev/null | grep -e ^* | sed -E  s/^\\\\\*\ \(.+\)$/\(\\\\\1\)\/`'
    local __prompt_tail="\[\033[35m\]$ "
    local __last_color="\[\033[00m\]"
    export PS1="\u@\h:\W$__git_branch_color$__git_branch$__prompt_tail$__last_color"

}

color_my_prompt

